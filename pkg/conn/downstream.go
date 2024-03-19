package conn

import (
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/magnm/pg0e/pkg/metrics"
	"github.com/magnm/pg0e/pkg/util"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

type DownstreamConnEntry struct {
	*C
	B              *pgproto3.Backend
	MessageQueue   chan pgproto3.Message
	Parameters     map[string]string
	Password       string
	State          DownstreamState
	inflight       *util.SyncedList[*Inflight]
	sessionQueries *util.SyncedList[*SessionQ]
	instrument     *DownstreamInstrument
	logger         *slog.Logger

	shouldPause   bool
	paused        bool
	onUnpause     chan bool
	onPaused      chan<- bool
	readyForQuery bool
}

type DownstreamMessageHandler func(pgproto3.FrontendMessage) error

type DownstreamState struct {
	Tx bool
	Idle bool
}

type DownstreamInstrument struct {
	Id          string
	QueryStart  time.Time
	UniqQueries map[uint32]bool
}

type Inflight struct {
	Query *SessionQ
}

type PersistKind string

const (
	TxBegin   PersistKind = "begin"
	TxEnd     PersistKind = "end"
	Set       PersistKind = "set"
	Unset     PersistKind = "unset"
	Prepare   PersistKind = "prepare"
	Unprepare PersistKind = "unprepare"
	Parse     PersistKind = "parse"
	Unparse   PersistKind = "unparse"
	Listen    PersistKind = "listen"
	Unlisten  PersistKind = "unlisten"
)

type SessionQ struct {
	Kind  PersistKind
	Ident string
	OIDs  []uint32
	Query string
}

func NewDownstream(conn net.Conn) (*DownstreamConnEntry, error) {
	entry := &DownstreamConnEntry{
		C:            NewConn(conn),
		B:            pgproto3.NewBackend(conn, conn),
		MessageQueue: make(chan pgproto3.Message, 50000),

		sessionQueries: util.NewSyncedList[*SessionQ](),
		inflight:       util.NewSyncedList[*Inflight](),
		instrument: &DownstreamInstrument{
			Id:          uuid.NewString(),
			UniqQueries: make(map[uint32]bool),
		},
		logger: slog.With("addr", conn.RemoteAddr().String(), "conn", "downstream"),
	}

	if err := entry.Startup(); err != nil {
		return nil, error
	}

	return entry, nil
}

func (d *DownstreamConnEntry) Close() error {
	return d.Conn.Close()
}
func (d *DownstreamConnEntry) Listen(handler DownstreamMessageHandler) {
	d.logger.Debug("downstream listening")
	for {
		msg, err := d.B.Receive()
		if err != nil {
			d.Term <- err
			return
		}

		if d.shouldPause && d.readyForQuery && !d.State.Tx {
			d.paused = true
			if d.onPaused != nil {
				d.onPaused <- true
			}
			d.logger.Debug("downstream paused", "immediate", false)
			<-d.onUnpause
			d.paused = false
			d.logger.Debug("downstream unpaused")
		}

		if err := handler(msg); err != nil {
			d.logger.Error("downstream message handler error", "err", err.Error())
			d.Term <- err
			return
		}
	}
}

func (d *DownstreamConnEntry) Send(msg pgproto3.BackendMessage) error {
	d.B.Send(msg)
	switch msg.(type) {
	case *pgproto3.DataRow:
		return nil
	default:
		return d.B.Flush()
	}
}

func (d *DownstreamConnEntry) SendTerminalError() error {
	var err error
	if err = d.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  "upstream is terminating",
		Code:     "57014",
	}); err != nil {
		return err
	}
	if d.State.Tx {
		err = d.Send(&pgproto3.ReadyForQuery{TxStatus: 'E'})
	} else {
		err = d.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	}
	d.readyForQuery = true
	d.State.Tx = false
	return err
}

func (d *DownstreamConnEntry) IsInSafeState() bool {
	return !d.State.Tx && d.State.Idle
}

func (d *DownstreamConnEntry) Pause(cb chan<- bool) {
	if !d.shouldPause {
		d.onUnpause = make(chan bool, 1)
		d.shouldPause = true
		if !d.State.Tx && d.readyForQuery {
			d.logger.Debug("downstream paused", "immediate", true)
			d.paused = true
			if cb != nil {
				cb <- true
			}
		} else {
			d.onPaused = cb
		}

		// Ensure that downstream is not paused too long no matter what
		go func() {
			timeout := time.After(12 * time.Second)
			select {
			case <-timeout:
				if d.shouldPause || d.paused {
					d.logger.Warn("downstream pause timeout")
					d.Resume()
				}
			case <-d.onUnpause:
				// forward onUnpause to real listener if this one gets it first
				d.onUnpause <- true
				break
			}
		}()
	}
}

func (d *DownstreamConnEntry) IsPaused() bool {
	return d.paused
}

func (d *DownstreamConnEntry) Resume() {
	d.logger.Debug("downstream resuming")
	if d.shouldPause {
		d.shouldPause = false
		d.onUnpause <- true
	}
}

func (d *DownstreamConnEntry) Queries() *util.SyncedList[*SessionQ] {
	return d.sessionQueries
}

func (d *DownstreamConnEntry) Startup() {
	
}

func (d *DownstreamConnEntry) AnalyzeMessages() {
	for {
		select {
		case <-d.Term:
			return // terminate, errors handled elsewhere
		case msg := <-d.MessageQueue:
			d.AnalyzeMsg(msg)
		}
	}
}

func (d *DownstreamConnEntry) AnalyzeMsg(msg pgproto3.Message) {
	instrTimeStart := time.Now()
	switch msg := msg.(type) {
	case *pgproto3.DataRow:
		return // instantly ignore datarows
	case pgproto3.FrontendMessage:
		d.AnalyzeRequestMsg(msg)
	case pgproto3.BackendMessage:
		d.AnalyzeResponseMsg(msg)
	}
	metrics.RecAnalyzeTime(float64(time.Since(instrTimeStart).Milliseconds()))
}

func (d *DownstreamConnEntry) AnalyzeRequestMsg(msg pgproto3.FrontendMessage) {
	d.logger.Debug("downstream req", "payload", msg)

	switch msg := (msg).(type) {
	case *pgproto3.Query:
		d.readyForQuery = false
		metrics.IncQuerySend(d.instrument.Id)
		d.instrument.QueryStart = time.Now()
		d.instrument.UniqQueries[util.HashString(msg.String)] = true

		query, err := pg_query.Parse(msg.String)
		if err != nil {
			d.logger.Warn("failed to parse query", "err", err.Error(), "query", msg.String)
			return
		}
		persistable := parsePersistableQueries(query.Stmts)
		if len(persistable) > 0 {
			d.logger.Debug("query persistable", "persistable", persistable)
			for _, persist := range persistable {
				d.inflight.Add(&Inflight{Query: &persist})
			}
		}
	case *pgproto3.Parse:
		d.readyForQuery = false
		if msg.Name == "" {
			query, err := pg_query.Parse(msg.Query)
			if err != nil {
				d.logger.Warn("failed to parse parse-query", "err", err.Error(), "query", msg.Query)
				return
			}
			persistable := parsePersistableQueries(query.Stmts)
			if len(persistable) > 0 {
				d.logger.Debug("parsed persistable", "persistable", persistable)
				for _, persist := range persistable {
					d.inflight.Add(&Inflight{Query: &persist})
				}
			}
		} else {
			d.inflight.Add(&Inflight{Query: &SessionQ{Kind: Parse, Ident: msg.Name, Query: msg.Query, OIDs: msg.ParameterOIDs}})
		}
		d.instrument.UniqQueries[util.HashString(msg.Query)] = true
	case *pgproto3.Execute:
		metrics.IncQuerySend(d.instrument.Id)
		d.instrument.QueryStart = time.Now()
	case *pgproto3.Close:
		d.inflight.Add(&Inflight{Query: &SessionQ{Kind: Unparse, Ident: msg.Name}})
	}
}

func (d *DownstreamConnEntry) AnalyzeResponseMsg(msg pgproto3.BackendMessage) {
	d.logger.Debug("upstream resp", "payload", msg)

	switch msg.(type) {
	case *pgproto3.ReadyForQuery:
		d.readyForQuery = true
	case *pgproto3.ParseComplete:
		for inflight := range d.inflight.Each() {
			switch inflight.Value.Query.Kind {
			case Parse:
				d.sessionQueries.Add(inflight.Value.Query)
			}
		}
	case *pgproto3.CloseComplete:
		for inflight := range d.inflight.Each() {
			switch inflight.Value.Query.Kind {
			case Unparse:
				d.sessionQueries.RemoveFirst(func(query *SessionQ) bool {
					return query.Kind == Parse && query.Ident == inflight.Value.Query.Ident
				})
			}
		}
	case *pgproto3.CommandComplete:
		d.finalizeInflight()
		metrics.IncQueryRecv(d.instrument.Id)
		metrics.RecQueryTime(time.Since(d.instrument.QueryStart).Seconds())
		d.logger.Debug("query time", "duration", time.Since(d.instrument.QueryStart).Seconds())

	case *pgproto3.ErrorResponse:
		d.inflight.Clear()
		d.State.Tx = false
		metrics.IncQueryErr(d.instrument.Id)
		metrics.RecQueryTime(time.Since(d.instrument.QueryStart).Seconds())
	}
}

func (d *DownstreamConnEntry) finalizeInflight() {
	for inflight := range d.inflight.EachA(func([]*Inflight) []*Inflight { return []*Inflight{} }) {
		persist := inflight.Value.Query
		switch persist.Kind {
		case TxBegin:
			d.State.Tx = true
		case TxEnd:
			d.State.Tx = false
		case Set, Prepare, Parse, Listen:
			d.sessionQueries.Add(persist)
		case Unset, Unprepare, Unparse, Unlisten:
			kind := strings.TrimPrefix(string(persist.Kind), "un")
			if persist.Ident == "*" {
				d.sessionQueries.Remove(func(query *SessionQ) bool {
					return query.Kind == PersistKind(kind)
				})
			} else {
				d.sessionQueries.RemoveFirst(func(query *SessionQ) bool {
					return query.Kind == PersistKind(kind) && query.Ident == persist.Ident
				})
			}
		}
	}
	d.logger.Debug("downstream current", "state", d.State, "sessionQueries", d.sessionQueries.UnsafeList())
}

func deparse(node *pg_query.Node) (string, error) {
	parseResult := pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{Stmt: node},
		},
	}
	return pg_query.Deparse(&parseResult)
}

func parsePersistableQueries(stmts []*pg_query.RawStmt) []SessionQ {
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
			oids := []uint32{}
			for _, oid := range node.PrepareStmt.Argtypes {
				oids = append(oids, oid.GetTypeName().GetTypeOid())
			}
			sessionQs = append(sessionQs, SessionQ{Kind: Prepare, Ident: node.PrepareStmt.Name, Query: query, OIDs: oids})
		case *pg_query.Node_DeallocateStmt:
			name := node.DeallocateStmt.Name
			if name == "" {
				name = "*"
			}
			sessionQs = append(sessionQs, SessionQ{Kind: Unprepare, Ident: name})
		case *pg_query.Node_VariableSetStmt:
			if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_VALUE {
				if !node.VariableSetStmt.GetIsLocal() {
					query, _ := deparse(raw.Stmt)
					sessionQs = append(sessionQs, SessionQ{Kind: Set, Ident: node.VariableSetStmt.Name, Query: query})
				}
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_DEFAULT {
				if !node.VariableSetStmt.GetIsLocal() {
					sessionQs = append(sessionQs, SessionQ{Kind: Unset, Ident: node.VariableSetStmt.Name})
				}
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_CURRENT {
				query, _ := deparse(raw.Stmt)
				sessionQs = append(sessionQs, SessionQ{Kind: Set, Ident: node.VariableSetStmt.Name, Query: query})
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET {
				sessionQs = append(sessionQs, SessionQ{Kind: Unset, Ident: node.VariableSetStmt.Name})
			} else if node.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET_ALL {
				sessionQs = append(sessionQs, SessionQ{Kind: Unset, Ident: "*"})
			}
		case *pg_query.Node_ListenStmt:
			query, _ := deparse(raw.Stmt)
			sessionQs = append(sessionQs, SessionQ{Kind: Listen, Ident: node.ListenStmt.Conditionname, Query: query})
		case *pg_query.Node_UnlistenStmt:
			if node.UnlistenStmt.Conditionname == "" {
				sessionQs = append(sessionQs, SessionQ{Kind: Unlisten, Ident: "*"})
			} else {
				sessionQs = append(sessionQs, SessionQ{Kind: Unlisten, Ident: node.UnlistenStmt.Conditionname})
			}
		}
	}
	return sessionQs
}
