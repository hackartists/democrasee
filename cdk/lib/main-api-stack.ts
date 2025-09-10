import {
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_elasticloadbalancingv2 as elbv2,
  aws_iam as iam,
} from "aws-cdk-lib";
import { RegionalServiceStack } from "./regional-service-stack";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as cdk from "aws-cdk-lib";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as apigw from "aws-cdk-lib/aws-apigateway";

export interface MainApiStackProps {
  prefix: string;
  latencyDomain: string;
  pghost: string;

  cert: acm.Certificate;
}

export class MainApiStack {
  constructor(scope: RegionalServiceStack, props: MainApiStackProps) {
    const { prefix, latencyDomain, pghost, cert } = props;
    const { zone, region } = scope;

    const codePath = ".build/main-api";

    const func = new lambda.Function(scope, "Function", {
      runtime: lambda.Runtime.PROVIDED_AL2023,
      code: lambda.Code.fromAsset(codePath),
      handler: "bootstrap",
      environment: {
        NO_COLOR: "true",
        PGHOST: pghost,
      },
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
    });

    const api = new apigateway.LambdaRestApi(scope, `${prefix}ApiGateway`, {
      handler: func,
      proxy: true,
    });

    const domain = new apigw.DomainName(scope, `${prefix}Domain`, {
      domainName: latencyDomain,
      certificate: cert,
    });

    new apigw.BasePathMapping(scope, `${prefix}Mapping`, {
      domainName: domain,
      restApi: api,
      basePath: "",
      stage: api.deploymentStage,
    });

    new route53.CfnRecordSet(scope, `${prefix}LatencyARecord`, {
      hostedZoneId: zone.hostedZoneId,
      name: latencyDomain,
      type: "A",
      setIdentifier: `${prefix}-${region}`,
      region: region,
      aliasTarget: {
        dnsName: domain.domainNameAliasDomainName,
        hostedZoneId: domain.domainNameAliasHostedZoneId,
        evaluateTargetHealth: true,
      },
    });

    new route53.CfnRecordSet(scope, `${prefix}LatencyAAAARecord`, {
      hostedZoneId: zone.hostedZoneId,
      name: latencyDomain,
      type: "AAAA",
      setIdentifier: `${prefix}-${region}`,
      region: region,
      aliasTarget: {
        dnsName: domain.domainNameAliasDomainName,
        hostedZoneId: domain.domainNameAliasHostedZoneId,
        evaluateTargetHealth: true,
      },
    });
  }
}
