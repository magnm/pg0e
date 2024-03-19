package cnpg

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/magnm/pg0e/pkg/interfaces"
	kubernetes "github.com/magnm/pg0e/pkg/kube/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

const PhaseWaitingForUserAction = "Waiting for user action"
const PhaseSwitchoverInProgress = "Switchover in progress"

var ErrNoHealhtyReplicas = errors.New("no healthy replicas")

var gvr = schema.GroupVersionResource{
	Group:    "postgresql.cnpg.io",
	Version:  "v1",
	Resource: "clusters",
}

type CNPGOrchestrator struct {
	server interfaces.Server
	logger *slog.Logger

	dynamicClient  *dynamic.DynamicClient
	dynamicFactory dynamicinformer.DynamicSharedInformerFactory
}

func New(server interfaces.Server) *CNPGOrchestrator {
	dynamicClient, err := kubernetes.GetKubernetesDynamicClient()
	if err != nil {
		panic(err)
	}

	c := &CNPGOrchestrator{
		server:        server,
		dynamicClient: dynamicClient,
		logger:        slog.With("orch", "cnpg"),
	}

	c.dynamicFactory = dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, time.Minute*5)
	informer := c.dynamicFactory.ForResource(gvr).Informer()

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldO := oldObj.(*unstructured.Unstructured)
			oldName, _, _ := unstructured.NestedString(oldO.Object, "metadata", "name")
			oldNs, _, err := unstructured.NestedString(oldO.Object, "metadata", "namespace")
			if err != nil {
				c.logger.Error("failed to get name/ns", "err", err)
				return
			}
			if oldName != os.Getenv("CNPG_CLUSTER_NAME") || oldNs != os.Getenv("CNPG_NAMESPACE") {
				c.logger.Debug("ignoring update, not configured name/ns", "name", oldName, "ns", oldNs)
				return
			}

			oldPhase, ok, err := unstructured.NestedString(oldO.Object, "status", "phase")
			if !ok || err != nil {
				c.logger.Error("failed to get old phase", "ok", ok, "err", err)
				return
			}
			newO := newObj.(*unstructured.Unstructured)
			newPhase, ok, err := unstructured.NestedString(newO.Object, "status", "phase")
			if !ok || err != nil {
				c.logger.Error("failed to get new phase", "ok", ok, "err", err)
				return
			}
			if oldPhase == newPhase {
				return
			}
			c.logger.Info("phase change", "old", oldPhase, "new", newPhase)
			if oldPhase != PhaseWaitingForUserAction && newPhase == PhaseWaitingForUserAction {
				c.server.InitiateSwitch()
			}
		},
	})

	return c
}

func (c *CNPGOrchestrator) Start() error {
	c.dynamicFactory.Start(nil)
	return nil
}

func (c *CNPGOrchestrator) Stop() error {
	return nil
}

func (c *CNPGOrchestrator) GetServer() interfaces.Server {
	return c.server
}

func (c *CNPGOrchestrator) TriggerSwitchover() error {
	namespace := os.Getenv("CNPG_NAMESPACE")
	clusterName := os.Getenv("CNPG_CLUSTER_NAME")
	resource, err := c.dynamicClient.
		Resource(gvr).
		Namespace(namespace).
		Get(context.Background(), clusterName, metav1.GetOptions{}, "status")
	if err != nil {
		return err
	}

	currentPrimary, ok, err := unstructured.NestedString(resource.Object, "status", "currentPrimary")
	if !ok || err != nil {
		return err
	}
	healthyReplicas, ok, err := unstructured.NestedSlice(resource.Object, "status", "instancesStatus", "healthy")
	if !ok || err != nil {
		return err
	}
	var targetPrimary string
	for _, r := range healthyReplicas {
		if r.(string) != currentPrimary {
			targetPrimary = r.(string)
			break
		}
	}
	if targetPrimary == "" {
		return ErrNoHealhtyReplicas
	}

	if err = unstructured.SetNestedField(resource.Object, PhaseSwitchoverInProgress, "status", "phase"); err != nil {
		return err
	}
	if err = unstructured.SetNestedField(resource.Object, "Switching over to "+targetPrimary, "status", "phaseReason"); err != nil {
		return err
	}
	if err = unstructured.SetNestedField(resource.Object, targetPrimary, "status", "targetPrimary"); err != nil {
		return err
	}
	if err = unstructured.SetNestedField(resource.Object, time.Now().Format(time.RFC3339), "status", "targetPrimaryTimestamp"); err != nil {
		return err
	}

	_, err = c.dynamicClient.
		Resource(gvr).
		Namespace(namespace).
		UpdateStatus(context.Background(), resource, metav1.UpdateOptions{})

	return err
}
