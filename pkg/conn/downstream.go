package conn

import (
	"log/slog"
	"net"
	"strings"

	"golang.org/x/exp/maps"

	"github.com/jackc/pgx/v5/pgproto3"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

type DownstreamConnEntry struct {
	C
	B              *pgproto3.Backend
	Data           chan pgproto3.FrontendMessage
	Parameters     map[string]string
	Password       string
	sessionQueries map[string]SessionQ
	paused         bool
	unpause        chan bool
	inTxn          bool
}

type PersistKind string

const (
	TxBegin   PersistKind = "begin"
	TxEnd     PersistKind = "end"
	Set       PersistKind = "set"
	Unset     PersistKind = "unset"
	Prepare   PersistKind = "prepare"
	Unprepare PersistKind = "unprepare"
	Listen    PersistKind = "listen"
	Unlisten  PersistKind = "unlisten"
)

type SessionQ struct {
	Kind  PersistKind
	Ident string
	Query string
}

func NewDownstreamEntry(conn net.Conn) *DownstreamConnEntry {
	return &DownstreamConnEntry{
		C:              *NewConn(conn),
		B:              pgproto3.NewBackend(conn, conn),
		Data:           make(chan pgproto3.FrontendMessage, 100),
		unpause:        make(chan bool, 1),
		sessionQueries: make(map[string]SessionQ),
	}
}

func (d *DownstreamConnEntry) Close() error {
	return d.Conn.Close()
}
func (d *DownstreamConnEntry) Listen() {
	slog.Debug("downstream listening", "addr", d.Conn.RemoteAddr().String())
	for {
		msg, err := d.B.Receive()
		if err != nil {
			d.Term <- err
			return
		}
		slog.Debug("downstream recv", "msg", msg)
		go d.AnalyzeMsg(msg)
		if d.paused {
			slog.Debug("downstream paused")
			<-d.unpause
			slog.Debug("downstream unpaused")
		}
		d.Data <- msg
	}
}

func (d *DownstreamConnEntry) Send(msg pgproto3.BackendMessage) error {
	d.B.Send(msg)
	return d.B.Flush()
}

func (d *DownstreamConnEntry) Pause() {
	if !d.paused {
		d.unpause = make(chan bool, 1)
		d.paused = true
	}
}

func (d *DownstreamConnEntry) Resume() {
	if d.paused {
		d.paused = false
		d.unpause <- true
	}
}

func (d *DownstreamConnEntry) Queries() []SessionQ {
	return maps.Values(d.sessionQueries)
}

func (d *DownstreamConnEntry) AnalyzeMsg(msg pgproto3.FrontendMessage) {
	switch msg := (msg).(type) {
	case *pgproto3.Query:
		query, err := pg_query.Parse(msg.String)
		if err != nil {
			slog.Warn("failed to parse query", "err", err.Error(), "query", msg.String)
			return
		}
		persists := parsePersistQueries(query.Stmts)
		if len(persists) > 0 {
			slog.Debug("query persists", "persists", persists)
			for _, persist := range persists {
				switch persist.Kind {
				case TxBegin:
					slog.Debug("txn begin")
					d.inTxn = true
				case TxEnd:
					slog.Debug("txn end")
					d.inTxn = false
				case Set, Prepare:
					d.sessionQueries[string(persist.Kind)+persist.Ident] = persist
				case Unset:
					if persist.Ident == "" {
						for k := range d.sessionQueries {
							if strings.HasPrefix(string(k), string(Set)) {
								delete(d.sessionQueries, k)
							}
						}
					} else {
						delete(d.sessionQueries, string(Set)+persist.Ident)
					}
				case Unprepare:
					delete(d.sessionQueries, string(Prepare)+persist.Ident)
				case Listen:
					d.sessionQueries[string(Listen)+persist.Ident] = persist
				case Unlisten:
					if persist.Ident == "" {
						for k := range d.sessionQueries {
							if strings.HasPrefix(string(k), string(Listen)) {
								delete(d.sessionQueries, k)
							}
						}
					} else {
						delete(d.sessionQueries, string(Listen)+persist.Ident)
					}
				}
			}
		}
	}
}

func deparse(node *pg_query.Node) (string, error) {
	parseResult := pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{Stmt: node},
		},
	}
	return pg_query.Deparse(&parseResult)
}

func parsePersistQueries(stmts []*pg_query.RawStmt) []SessionQ {
	sessionQs := []SessionQ{}
	for _, raw := range stmts {
		switch node := raw.Stmt.GetNode().(type) {
		case *pg_query.Node_TransactionStmt:
			if node.TransactionStmt.Kind == pg_query.TransactionStmtKind_TRANS_STMT_BEGIN {
				sessionQs = append(sessionQs, SessionQ{Kind: TxBegin})
			} else if node.TransactionStmt.Kind == pg_query.TransactionStmtKind_TRANS_STMT_START {
				sessionQs = append(sessionQs, SessionQ{Kind: TxBegin})
			} else if node.TransactionStmt.Kind == pg_query.TransactionStmtKind_TRANS_STMT_COMMIT {
				sessionQs = append(sessionQs, SessionQ{Kind: TxEnd})
			} else if node.TransactionStmt.Kind == pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK {
				sessionQs = append(sessionQs, SessionQ{Kind: TxEnd})
			}
		case *pg_query.Node_PrepareStmt:
			query, _ := deparse(raw.Stmt)
			sessionQs = append(sessionQs, SessionQ{Kind: Prepare, Ident: node.PrepareStmt.Name, Query: query})
		case *pg_query.Node_DeallocateStmt:
			sessionQs = append(sessionQs, SessionQ{Kind: Unprepare, Ident: node.DeallocateStmt.Name})
		case *pg_query.Node_VariableSetStmt:
			if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_VALUE {
				query, _ := deparse(raw.Stmt)
				sessionQs = append(sessionQs, SessionQ{Kind: Set, Ident: node.VariableSetStmt.Name, Query: query})
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_DEFAULT {
				sessionQs = append(sessionQs, SessionQ{Kind: Unset, Ident: node.VariableSetStmt.Name})
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_CURRENT {
				query, _ := deparse(raw.Stmt)
				sessionQs = append(sessionQs, SessionQ{Kind: Set, Ident: node.VariableSetStmt.Name, Query: query})
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET {
				sessionQs = append(sessionQs, SessionQ{Kind: Unset, Ident: node.VariableSetStmt.Name})
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET_ALL {
				sessionQs = append(sessionQs, SessionQ{Kind: Unset})
			}
		case *pg_query.Node_ListenStmt:
			query, _ := deparse(raw.Stmt)
			sessionQs = append(sessionQs, SessionQ{Kind: Listen, Ident: node.ListenStmt.Conditionname, Query: query})
		case *pg_query.Node_UnlistenStmt:
			if node.UnlistenStmt.Conditionname == "*" {
				sessionQs = append(sessionQs, SessionQ{Kind: Unlisten})
			} else {
				sessionQs = append(sessionQs, SessionQ{Kind: Unlisten, Ident: node.UnlistenStmt.Conditionname})
			}
		}
	}
	return sessionQs
}
