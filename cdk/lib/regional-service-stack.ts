import {
  Stack,
  StackProps,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_ecs_patterns as ecs_patterns,
  aws_elasticloadbalancingv2 as elbv2,
  aws_route53 as route53,
  aws_certificatemanager as acm,
  aws_cloudfront as cloudfront,
  aws_cloudfront_origins as origins,
  aws_route53_targets as targets,
  aws_iam as iam,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import { DaemonStack } from "./daemon-stack";
import { WebStack } from "./web-stack";
import { MainApiStack } from "./main-api-stack";

export interface RegionalServiceStackProps extends StackProps {
  // Domain parts, e.g. "dev2.ratel.foundation"
  fullDomainName: string;
  // Health check path for ALB target group
  healthCheckPath?: string;
  commit: string;
  // Repository names
  apiRepoName?: string;
  webRepoName?: string;
  minCapacity?: number;
  maxCapacity?: number;
  enableDaemon?: boolean;
  pghost: string;

  webLatencyDomain: string;
  apiLatencyDomain: string;

  cert: acm.Certificate;
}

export class RegionalServiceStack extends Stack {
  public readonly webstack: WebStack;
  public readonly mainApiStack: MainApiStack;
  public readonly zone: route53.IHostedZone;

  constructor(scope: Construct, id: string, props: RegionalServiceStackProps) {
    super(scope, id, { ...props, crossRegionReferences: true });

    const { webLatencyDomain, apiLatencyDomain, pghost, cert } = props;

    const domain = props.fullDomainName;
    const baseDomain = "ratel.foundation";

    const vpc = ec2.Vpc.fromLookup(this, "DefaultVpc", { isDefault: true });

    const cluster = new ecs.Cluster(this, "Cluster", { vpc });

    const rootZone = route53.HostedZone.fromLookup(this, "RootZone", {
      domainName: baseDomain,
    });
    this.zone = rootZone;

    if (props.enableDaemon) {
      new DaemonStack(this, {
        vpc,
        cluster,
        commit: props.commit,
      });
    }

    this.webstack = new WebStack(this, {
      prefix: "Web",
      latencyDomain: webLatencyDomain,
      githubRepo: process.env.GITHUB_REPO || "https://github.com/biyard/ratel",
      githubBranch: process.env.GITHUB_BRANCH || "dev",
      githubAccessToken: process.env.GITHUB_ACCESS_TOKEN!,
      domain,
      cert,
    });
    this.mainApiStack = new MainApiStack(this, {
      prefix: "Api",
      latencyDomain: apiLatencyDomain,
      pghost,
      cert,
    });
  }
}
