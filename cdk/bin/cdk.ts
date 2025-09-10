import { App } from "aws-cdk-lib";
import { RegionalServiceStack } from "../lib/regional-service-stack";
import { GlobalAccelStack } from "../lib/global-accel-stack";

const app = new App();

const stackName = process.env.STACK;

const env = process.env.ENV || "dev";
const host = process.env.DOMAIN || "dev2.ratel.foundation";
const webLatencyDomain = host; // Use the main domain directly
const apiLatencyDomain = `api.${host}`;

const { cert } = new GlobalAccelStack(app, "GlobalAccel", {
  stackName,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: "us-east-1",
  },
  fullDomainName: host,
});

const apStack = new RegionalServiceStack(
  app,
  `ratel-${env}-svc-ap-northeast-2`,
  {
    env: {
      account: process.env.CDK_DEFAULT_ACCOUNT,
      region: "ap-northeast-2",
    },
    fullDomainName: host,
    healthCheckPath: "/version",
    commit: process.env.COMMIT!,
    pghost: process.env.PGHOST_AP!,
    enableDaemon: true,

    webLatencyDomain,
    apiLatencyDomain,
    cert,
  },
);

const euStack = new RegionalServiceStack(app, `ratel-${env}-svc-eu-central-1`, {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: "eu-central-1",
  },
  fullDomainName: host,
  healthCheckPath: "/version",
  commit: process.env.COMMIT!,
  pghost: process.env.PGHOST_EU!,

  webLatencyDomain,
  apiLatencyDomain,
  cert,
});

const usStack = new RegionalServiceStack(app, `ratel-${env}-svc-us-east-1`, {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: "us-east-1",
  },
  fullDomainName: host,
  healthCheckPath: "/version",
  commit: process.env.COMMIT!,
  pghost: process.env.PGHOST_US!,

  webLatencyDomain,
  apiLatencyDomain,
  cert,
});
