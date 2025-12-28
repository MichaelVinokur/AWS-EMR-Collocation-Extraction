package org.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class App {
    public static void main(String[] args) {
        String bucketName = "mtmv-bguproject";
        String jarPath = "s3://" + bucketName + "/CollocationProject/code/collocation-extraction-EMR-1.0-SNAPSHOT.jar";
        //hebrew
        //String stopWordsPath = "s3://" + bucketName + "/CollocationProject/config/heb-stopwords.txt";
        //String inputPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
        //String outputPath = "s3://" + bucketName + "/CollocationProject/output/hebrew_run_" + System.currentTimeMillis();
        //english
        String stopWordsPath = "s3://" + bucketName + "/CollocationProject/config/eng-stopwords.txt";
        String inputPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"; //smaller than in the assigment
        String outputPath = "s3://" + bucketName + "/CollocationProject/output/outputeng/english_run_" + System.currentTimeMillis();


        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String sessionToken = System.getenv("AWS_SESSION_TOKEN");

        AWSCredentialsProvider credentialsProvider;
        if (sessionToken != null && !sessionToken.isEmpty()) {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicSessionCredentials(accessKey, secretKey, sessionToken));
        } else {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion("us-east-1")
                .withCredentials(credentialsProvider)
                .build();


        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("org.example.collocationExtraction")
                //hebrew
                //.withArgs(inputPath, outputPath, "hebrew", stopWordsPath);
                //english
                .withArgs(inputPath, outputPath, "english", stopWordsPath);
        StepConfig stepConfig = new StepConfig()
                .withName("Collocation Extraction Step")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withKeepJobFlowAliveWhenNoSteps(false) //close cluster at the end
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Collocation Extraction Job")
                .withReleaseLabel("emr-5.20.0")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3://" + bucketName + "/CollocationProject/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}