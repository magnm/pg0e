package cnpg

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/magnm/pg0e/pkg/interfaces"
	kubernetes "github.com/magnm/pg0e/pkg/kube/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

const PhaseWaitingForUserAction = "Waiting for user action"
const PhaseSwitchoverInProgress = "Switchover in progress"

var ErrNoHealhtyReplicas = errors.New("no healthy replicas")

var gvr = schema.GroupVersionResource{
	Group:    "cnpg.io",
	Version:  "v1",
	Resource: "Cluster",
}

type CNPGOrchestrator struct {
	server interfaces.Server

	dynamicClient  *dynamic.DynamicClient
	dynamicFactory dynamicinformer.DynamicSharedInformerFactory
}

type ClusterSwitchoverUpdate struct {
	Status ClusterSwitchoverUpdateStatus `json:"status"`
}

type ClusterSwitchoverUpdateStatus struct {
	Phase                  string `json:"phase"`
	PhaseReason            string `json:"phaseReason"`
	TargetPrimary          string `json:"targetPrimary"`
	TargetPrimaryTimestamp string `json:"targetPrimaryTimestamp"`
}

func New(server interfaces.Server) *CNPGOrchestrator {
	dynamicClient, err := kubernetes.GetKubernetesDynamicClient()
	if err != nil {
		panic(err)
	}

	c := &CNPGOrchestrator{
		server:        server,
		dynamicClient: dynamicClient,
	}

	c.dynamicFactory = dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, time.Minute*5)
	informer := c.dynamicFactory.ForResource(gvr).Informer()

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldO := newObj.(*unstructured.Unstructured)
			oldPhase, ok, err := unstructured.NestedString(oldO.Object, "status", "phase")
			if !ok || err != nil {
				return
			}
			newO := newObj.(*unstructured.Unstructured)
			newPhase, ok, err := unstructured.NestedString(newO.Object, "status", "phase")
			if !ok || err != nil {
				return
			}
			if oldPhase != PhaseWaitingForUserAction && newPhase == PhaseWaitingForUserAction {
				c.server.InitiateSwitch()
			}
		},
	})

	return c
}

func (c *CNPGOrchestrator) Start() error {
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

	update := &ClusterSwitchoverUpdate{
		Status: ClusterSwitchoverUpdateStatus{
			Phase:                  PhaseSwitchoverInProgress,
			PhaseReason:            "Switching over to " + targetPrimary,
			TargetPrimary:          targetPrimary,
			TargetPrimaryTimestamp: time.Now().Format(time.RFC3339),
		},
	}
	jsonUpdate, err := json.Marshal(update)
	if err != nil {
		return err
	}

	_, err = c.dynamicClient.
		Resource(gvr).
		Namespace(namespace).
		Patch(context.Background(), clusterName, types.JSONPatchType, jsonUpdate, metav1.PatchOptions{})
	return err
}
