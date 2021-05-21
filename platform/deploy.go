package platform

import (
	dataproc "cloud.google.com/go/dataproc/apiv1"
	"context"
	"fmt"
	"google.golang.org/api/option"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	"log"
	"os"
	"regexp"

	"github.com/hashicorp/waypoint-plugin-sdk/terminal"
)

type DeployConfig struct {
	Region string `hcl:"region"`
	ClusterName string `hcl:"cluster_name"`
	ProjectId string `hcl:"project_id,optional"`

	MainClass string `hcl:"main_class"`
	JobUri string `hcl:"job_uri"`
	MasterEnvVariables map[string]string `hcl:"master_env_variables,optional"`
	DriverEnvVariables map[string]string `hcl:"driver_env_variables,optional"`
	ExecutorEnvVariables map[string]string `hcl:"executor_env_variables,optional"`
	Arguments []string `hcl:"arguments,optional"`

}

type Platform struct {
	config DeployConfig
}

// Implement Configurable
func (p *Platform) Config() (interface{}, error) {
	return &p.config, nil
}

// Implement ConfigurableNotify
func (p *Platform) ConfigSet(config interface{}) error {
	c, ok := config.(*DeployConfig)
	if !ok {
		// The Waypoint SDK should ensure this never gets hit
		return fmt.Errorf("Expected *DeployConfig as parameter")
	}

	// validate the config
	if c.Region == "" {
		return fmt.Errorf("Region must be set to a valid directory")
	}

	if c.ProjectId == "" && os.Getenv("GOOGLE_PROJECT_ID") != "" {
		c.ProjectId = os.Getenv("GOOGLE_PROJECT_ID")
	}

	return nil
}

// Implement Builder
func (p *Platform) DeployFunc() interface{} {
	// return a function which will be called by Waypoint
	return p.deploy
}

// A BuildFunc does not have a strict signature, you can define the parameters
// you need based on the Available parameters that the Waypoint SDK provides.
// Waypoint will automatically inject parameters as specified
// in the signature at run time.
//
// Available input parameters:
// - context.Context
// - *component.Source
// - *component.JobInfo
// - *component.DeploymentConfig
// - *datadir.Project
// - *datadir.App
// - *datadir.Component
// - hclog.Logger
// - terminal.UI
// - *component.LabelSet

// In addition to default input parameters the registry.Artifact from the Build step
// can also be injected.
//
// The output parameters for BuildFunc must be a Struct which can
// be serialzied to Protocol Buffers binary format and an error.
// This Output Value will be made available for other functions
// as an input parameter.
// If an error is returned, Waypoint stops the execution flow and
// returns an error to the user.
func (b *Platform) deploy(ctx context.Context, ui terminal.UI) (*Deployment, error) {
	u := ui.Status()
	defer u.Close()
	u.Update("Deploy application")

	u.Step(terminal.InfoStyle, "Deploying Spark Job")

	u.Step(terminal.InfoStyle, "Region: " + b.config.Region)
	u.Step(terminal.InfoStyle, "Cluster Name: " + b.config.ClusterName)

	endpoint := fmt.Sprintf("%s-dataproc.googleapis.com:443", b.config.Region)
	jobClient, err := dataproc.NewJobControllerClient(ctx, option.WithEndpoint(endpoint))
	if err != nil {
		log.Fatalf("error creating the job client: %s\n", err)
	}

	properties := make(map[string]string)

	for key, value := range b.config.MasterEnvVariables {
		propertyKey := "spark.yarn.appMasterEnv." + key
		properties[propertyKey] = value
	}

	for key, value := range b.config.DriverEnvVariables {
		propertyKey := "spark.driverEnv." + key
		properties[propertyKey] = value
	}

	for key, value := range b.config.ExecutorEnvVariables {
		propertyKey := "spark.executorEnv." + key
		properties[propertyKey] = value
	}


	//// Create the job config.
	submitJobReq := &dataprocpb.SubmitJobRequest{
		ProjectId: b.config.ProjectId,
		Region:    b.config.Region,
		Job: &dataprocpb.Job{
			Placement: &dataprocpb.JobPlacement{
				ClusterName: b.config.ClusterName,
			},
			TypeJob: &dataprocpb.Job_SparkJob{
				SparkJob: &dataprocpb.SparkJob{
					Driver: &dataprocpb.SparkJob_MainClass{
						MainClass: b.config.MainClass,
					},
					Properties: properties,
					JarFileUris: []string{b.config.JobUri},
				},
			},
		},
	}
	submitJobOp, err := jobClient.SubmitJobAsOperation(ctx, submitJobReq)
	if err != nil {
		return nil, err
	}

	submitJobResp, err := submitJobOp.Wait(ctx)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile("gs://(.+?)/(.+)")
	matches := re.FindStringSubmatch(submitJobResp.DriverOutputResourceUri)

	if len(matches) < 3 {
		return nil, err
	}

	u.Step(terminal.SuccessStyle, "Spark Job Deployed")

	return &Deployment{}, nil
}
