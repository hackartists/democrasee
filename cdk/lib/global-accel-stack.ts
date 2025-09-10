import {
  Stack,
  StackProps,
  aws_route53 as route53,
  aws_certificatemanager as acm,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import * as cloudfront from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as cdk from "aws-cdk-lib";
import * as targets from "aws-cdk-lib/aws-route53-targets";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import { RegionalServiceStack } from "./regional-service-stack";

export interface GlobalAccelStackProps extends StackProps {
  fullDomainName: string;
}

export class GlobalAccelStack extends Stack {
  public readonly distribution: cloudfront.Distribution;
  public readonly cert: acm.Certificate;

  constructor(scope: Construct, id: string, props: GlobalAccelStackProps) {
    super(scope, id, { ...props, crossRegionReferences: true });

    const {
      fullDomainName, // , commit, webLatencyDomain, apiLatencyDomain
    } = props;

    const webDomain = fullDomainName;

    const apiDomain = `api.${fullDomainName}`;

    const baseDomain = "ratel.foundation";
    const zone = route53.HostedZone.fromLookup(this, "RootZone", {
      domainName: baseDomain,
    });

    this.cert = new acm.Certificate(this, "AlbCert", {
      domainName: webDomain,
      subjectAlternativeNames: [apiDomain],
      validation: acm.CertificateValidation.fromDns(zone),
    });

    // const origin = new origins.HttpOrigin(webLatencyDomain);
    // const apiOrigin = new origins.HttpOrigin(apiDomain);

    // // 1) S3 for static assets
    // const staticBucket = new s3.Bucket(this, "NextStaticBucket", {
    //   removalPolicy: cdk.RemovalPolicy.DESTROY,
    // });

    // const oai = new cloudfront.OriginAccessIdentity(this, "OAI");
    // staticBucket.grantRead(oai);

    // const s3Origin = origins.S3BucketOrigin.withOriginAccessIdentity(
    //   staticBucket,
    //   {
    //     originAccessIdentity: oai,
    //   },
    // );

    // const cachedNextProp = {
    //   origin,
    //   cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
    //   compress: true,
    // };

    // const apiProp = {
    //   origin: apiOrigin,
    //   cachePolicy: cloudfront.CachePolicy.CACHING_DISABLED,
    //   originRequestPolicy: cloudfront.OriginRequestPolicy.ALL_VIEWER,
    //   allowedMethods: cloudfront.AllowedMethods.ALLOW_ALL,
    //   viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
    // };

    // const s3Prop = {
    //   origin: s3Origin,
    //   cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
    //   viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
    //   compress: true,
    // };

    // this.distribution = new cloudfront.Distribution(this, "Distribution", {
    //   defaultBehavior: {
    //     origin,
    //     cachePolicy: cloudfront.CachePolicy.CACHING_DISABLED,
    //     originRequestPolicy: cloudfront.OriginRequestPolicy.ALL_VIEWER,
    //     allowedMethods: cloudfront.AllowedMethods.ALLOW_ALL,
    //     viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
    //   },
    //   additionalBehaviors: {
    //     "/v1/*": apiProp,
    //     "/v2/*": apiProp,
    //     "/version": apiProp,

    //     "/_next/image*": cachedNextProp,

    //     "/_next/static/*": s3Prop,
    //     "/metadata/*": s3Prop,
    //     "/assets/*": s3Prop,
    //     "/*.js": s3Prop,
    //     "/*.css": s3Prop,
    //     "/*.html": s3Prop,
    //     "/*.ico": s3Prop,
    //     "/*.svg": s3Prop,
    //     "/*.avif": s3Prop,
    //     "/*.png": s3Prop,
    //     "/*.wasm": s3Prop,
    //     "/icons/*": s3Prop,
    //     "/images/*": s3Prop,
    //     "/public/*": s3Prop,
    //   },

    //   domainNames: [webDomain],
    //   certificate: cert,
    //   httpVersion: cloudfront.HttpVersion.HTTP2_AND_3,
    //   priceClass: cloudfront.PriceClass.PRICE_CLASS_ALL,
    // });

    // // ---- Route53 alias for the end-user domain → CloudFront ----
    // new route53.ARecord(this, "AliasV4", {
    //   zone,
    //   recordName: webDomain.replace(`.${baseDomain}`, ""), // e.g., 'dev'
    //   target: route53.RecordTarget.fromAlias(
    //     new targets.CloudFrontTarget(this.distribution),
    //   ),
    // });
    // new route53.AaaaRecord(this, "AliasV6", {
    //   zone,
    //   recordName: webDomain.replace(`.${baseDomain}`, ""),
    //   target: route53.RecordTarget.fromAlias(
    //     new targets.CloudFrontTarget(this.distribution),
    //   ),
    // });

    // new s3deploy.BucketDeployment(this, "NextStaticDeployStatic", {
    //   destinationBucket: staticBucket,
    //   distribution: this.distribution,
    //   distributionPaths: ["/_next/static/*"],
    //   sources: [
    //     s3deploy.Source.asset(".build/web/static", {
    //       assetHash: commit,
    //       assetHashType: cdk.AssetHashType.CUSTOM,
    //     }),
    //   ],
    //   destinationKeyPrefix: "_next/static",
    // });

    // new s3deploy.BucketDeployment(this, "PublicDeployStatic", {
    //   destinationBucket: staticBucket,
    //   distribution: this.distribution,
    //   distributionPaths: ["/*"],
    //   sources: [
    //     s3deploy.Source.asset(".build/web/public", {
    //       assetHash: commit,
    //       assetHashType: cdk.AssetHashType.CUSTOM,
    //     }),
    //   ],
    // });
  }
}
