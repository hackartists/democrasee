import {
  Duration,
  Stack,
  StackProps,
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

export interface WebStackProps {
  prefix: string;
  regionalDomain: string;
  latencyDomain: string;
}

export class WebStack {
  public readonly domainName: string;
  public readonly latencyDomain: string;

  constructor(scope: RegionalServiceStack, props: WebStackProps) {
    const { prefix, regionalDomain, latencyDomain } = props;
    const { zone, region } = scope;

    const zipFilePath = ".build/web/lambda.zip";

    const func = new lambda.Function(scope, `${prefix}Function`, {
      runtime: lambda.Runtime.NODEJS_22_X,
      code: lambda.Code.fromAsset(zipFilePath),
      handler: "ts-packages/web/lambda.handler",
      environment: {
        NODE_ENV: "production",
      },
      memorySize: 512,
      timeout: cdk.Duration.seconds(30),
    });

    const api = new apigateway.LambdaRestApi(scope, `${prefix}ApiGateway`, {
      handler: func,
      proxy: true,
    });

    const cert = new acm.Certificate(scope, `${prefix}Cert`, {
      domainName: regionalDomain,
      validation: acm.CertificateValidation.fromDns(zone),
    });

    const domain = new apigw.DomainName(scope, `${prefix}Domain`, {
      domainName: regionalDomain,
      certificate: cert,
      endpointType: apigw.EndpointType.REGIONAL,
      securityPolicy: apigw.SecurityPolicy.TLS_1_2,
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
        evaluateTargetHealth: false,
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
        evaluateTargetHealth: false,
      },
    });

    this.latencyDomain = latencyDomain;
  }
}
