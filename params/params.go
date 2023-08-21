package params

import (
	"flag"
	"os"
)

var (
	Test       = flag.Bool("test", false, "Scratch pad, anything")
	Env        = flag.String("env", os.Getenv("RUN_ENV"), "Runtime environment (dev, next, prod)")
	Region     = flag.String("region", os.Getenv("AWS_REGION"), "AWS region")
	Key        = flag.String("key", os.Getenv("AWS_ACCESS_KEY_ID_ASSUME"), "AWS assumer access key")
	Secret     = flag.String("secret", os.Getenv("AWS_SECRET_ACCESS_KEY_ASSUME"), "AWS assumer secret access key")
	RoleArn    = flag.String("rolearn", os.Getenv("ROLE_ARN"), "AWS role ARN to assume")
	RoleArnAlm = flag.String("rolearnalm", os.Getenv("ROLE_ARN_ALM"), "AWS role ARN to assume (ALM)")
)
