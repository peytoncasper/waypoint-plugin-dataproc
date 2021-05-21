module github.com/peytoncasper/waypoint-plugin-dataproc

go 1.14

require (
	cloud.google.com/go v0.81.0
	github.com/hashicorp/waypoint-plugin-sdk v0.0.0-20210510195008-b42c688ebedf
	google.golang.org/api v0.47.0
	google.golang.org/genproto v0.0.0-20210513213006-bf773b8c8384
	google.golang.org/protobuf v1.26.0
)

// replace github.com/hashicorp/waypoint-plugin-sdk => ../../waypoint-plugin-sdk
