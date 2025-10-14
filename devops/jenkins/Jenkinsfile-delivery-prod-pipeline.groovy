@Library('jenkins-sharedlib@feature/dataops-master')

import sharedlib.DatabricksJenkinsUtil
def utils = new DatabricksJenkinsUtil(this)

import sharedlib.ToolsDatalakeJenkinsUtil
def utilsT = new ToolsDatalakeJenkinsUtil(this)

import sharedlib.utils.AzureUtil;
def azUtil = new AzureUtil(this, steps);

import sharedlib.azure.DatafactoryUtil
def utilsDF = new DatafactoryUtil(this, 'daily')

import sharedlib.PythonJenkinsUtil
def utilsPy = new PythonJenkinsUtil(this)
/* Project settings */
def project="LHCL"
def recipients="dummy@bcp.com.pe"
def deploymentEnvironment="prod"
def folderName = "src"
def codServicio='ml'
def GLOB_AMBT_COD = "PROD"
def AzOptions = ["subscriptionId": "096184b6-7b9d-468d-8fc8-fa7dad32cbb2"]
def subscriptionId = "096184b6-7b9d-468d-8fc8-fa7dad32cbb2"


def clusterName = ""
def hostDatabricks = "https://adb-4459256211327883.3.azuredatabricks.net"

try {
   node {

      stage('Preparation') {
         steps.step([$class: 'WsCleanup'])
         //utils.notifyByMail('START',recipients)
         checkout scm
         //Setup parameters
         env.project="${project}"
         utilsT.prepareLKDV("${workspace}","${GLOB_AMBT_COD}","process_cfg.json")
         env.codServicio = "${codServicio}"
         env.deploymentEnvironment="${deploymentEnvironment}"
         env.GLOB_AMBT_COD="${GLOB_AMBT_COD}"
         utilsDF.setIsPrepareEnviroment(true)
         utilsPy.setFlowPypiEnabled(true)
         utilsPy.withImagePython("PYTHON312")
         utilsPy.withImageSonar("PYTHON312_SPARK34_SONAR")
         utilsPy.withImageFortify("PYTHON37_FORTIFY")
         utils.prepare()
         utilsPy.prepare()
         applicationName = utilsPy.getApplicationName()
         utils.setPackageType("gz")
         utilsPy.setRelativeSrcPath("${folderName}")
         azUtil.setAzOptions(AzOptions)
         utilsDF.prepare()
          //*** inicio de configuracion hashicorp vault ***
         //****************** class toolsDatalake ***************
		   utilsT.setHashicorpVaultEnabled(true)
         utilsT.setHashicorpVaultEnvironment("prod")
         //****************** class pythonJenkinsUtil ***************
         utilsPy.setHashicorpVaultEnabled(true)
         utilsPy.setHashicorpVaultEnvironment("prod")
         //****************** class DatabricksJenkinsUtil ***************
         utils.setHashicorpVaultEnabled(true)
         utils.setHashicorpVaultEnvironment("prod")
         //****************** class AzureUtil ***************
         azUtil.setHashicorpVaultEnabled(true)
         azUtil.setHashicorpVaultEnvironment("prod")
      }
      stage('Start Release') {
         echo "inicio release"
         echo "params.RELEASE_TAG_NAME ${params.RELEASE_TAG_NAME}"
         utilsPy.promoteRelease(params.RELEASE_TAG_NAME,params.FORCE_RELEASE)
      }
      stage('Download Artifact'){
         utilsPy.downloadArtifactPypi()
      }
      stage('Deploy Azure Storage') {
         azUtil.adlsValidateDirectoryData(subscriptionId)
      }
      stage('Despliegue - Server'){
         utilsT.executeScriptsDDLaDMLPath("estrategia","scripts", "tmp")
      }

      stage('Deploy to Azure'){
        //Despliegue de elementos en el workspace
        utils.deployWorkspace(hostDatabricks);
        //Cración del jobs clusters workflows que hace un listado
        utils.createorUpdateJobWorkflowV2(hostDatabricks);
        // este metodo se  agrega solo en el caso de ser necesario
        //utils.executeJobWorkflow(hostDatabricks);
      }
      stage('Actualización de Archivo joblist.tmp'){
         utils.executeBkpJoblistV2(hostDatabricks)
      }

      stage('Ejecución de Proceso'){
		utilsT.executeprocesslhcl() //agregado
      }
     stage('Post Execution') {
        echo "****************Tipo de PASE : executePostExecutionTasks ********"
        utils.executePostExecutionTasks()
        echo "****************Tipo de PASE : notifyByMail ********"
        utils.notifyByMail('SUCCESS',recipients)
     }

   }
} catch(Exception e) {
   node{
      utils.executeOnErrorExecutionTasks()
      utils.notifyByMail('FAIL',recipients)
    throw e
   }
}
