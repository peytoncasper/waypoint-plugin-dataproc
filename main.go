package main

import (
	"github.com/peytoncasper/waypoint-plugin-dataproc/platform"
	sdk "github.com/hashicorp/waypoint-plugin-sdk"
)

func main() {
	sdk.Main(sdk.WithComponents(
		&platform.Platform{},
	))
}
