import com.jayway.jsonpath.JsonPath
import groovy.json.JsonBuilder
import groovy.xml.XmlUtil
import groovyx.net.http.*

import static groovyx.net.http.ContentTypes.JSON
import static groovyx.net.http.HttpVerb.DELETE
import static groovyx.net.http.HttpVerb.GET
import static groovyx.net.http.HttpVerb.POST
import static groovyx.net.http.HttpVerb.PUT
import static groovyx.net.http.MultipartContent.multipart

class NifiClient {

	def client
	def requestUri
	def templatesDir = new File("templates")

	NifiClient(host, port) {
		this(host, port, "http", null, null)
	}

	NifiClient(host, port, scheme, sslContext, hostnameVerifier) {
		this.requestUri = "${scheme}://${host}:${port}"
		println "Connecting to NiFi REST API at: " + this.requestUri
		this.client = OkHttpBuilder.configure {
			request.uri = this.requestUri
			execution.sslContext = sslContext
			execution.hostnameVerifier = hostnameVerifier
			execution.interceptor([GET, POST, PUT, DELETE] as HttpVerb[]) { ChainedHttpConfig cfg, java.util.function.Function<ChainedHttpConfig, Object> fx ->
				println cfg.request.verb.name() + ": " + cfg.request.uri.path
				return fx.apply(cfg)
			}
		}
	}

	def getUserId(identity) {
		def users = client.get { request.uri.path = "/nifi-api/tenants/users" }
		return JsonPath.parse(users).read("\$.users[?(@.component.identity == '${identity}')].component.id").get(0)
	}

	def exportTemplate(processGroupName) {
		def templateName = processGroupName + "-" + new Date().format("yyyy-MM-dd'T'HH:mm:ss")

		String flowId = getFlowId()
		String processGroupId = getProcessGroupId(flowId, processGroupName)
		println "Process Group ID: " + processGroupId

		String snippetId = createSnippet(flowId, processGroupId)
		String templateId = createTemplate(processGroupId, snippetId, templateName)
		println "Template ID: " + templateId

		def xmlTemplate = XmlUtil.serialize(downloadTemplate(templateId))
		writeTemplateToFile(xmlTemplate, processGroupName)
	}

	def uploadTemplate(templateFile) {
		String flowId = getFlowId()
		return client.post {
			request.uri.path = "/nifi-api/process-groups/${flowId}/templates/upload"
			request.contentType = "multipart/form-data"
			request.body = multipart {
				part "template", "template.xml", "text/xml", templateFile
			}
			request.encoder "multipart/form-data", OkHttpEncoders.&multipart
		}
	}

	def getTemplateNameFromFile(templateFile) {
		return new XmlSlurper().parse(templateFile).name
	}

	def getTemplateId(templateName) {
		return JsonPath.parse(getTemplates()).read("\$.templates[?(@.template.name == '${templateName}')].id").get(0)
	}

	def instantiateTemplate(templateId) {
		return instantiateTemplate(templateId, "200.0", "200.0")
	}

	def instantiateTemplate(theTemplateId, x, y) {
		JsonBuilder builder = new JsonBuilder()
		builder {
			originX x
			originY y
			templateId theTemplateId
		}

		String flowId = getFlowId()
		println "Instantiating template with ID: " + theTemplateId
		return client.post {
			request.uri.path = "/nifi-api/process-groups/${flowId}/template-instance"
			request.body = builder.toString()
			request.contentType = JSON[0]
		}
	}

	def deleteTemplatesStartingWith(processGroupName) {
		def templates = JsonPath.parse(getTemplates()).read("\$.templates[?(@.template.name =~ /${processGroupName}.*/i)]")
		if (!templates.isEmpty()) {
			println "Found ${templates.size()} templates for process group: ${processGroupName}"
			templates.each { template ->
				println "Deleting template: " + template.id
				deleteTemplate(template.id)
			}
			println "Finished deleting templates for process group: ${processGroupName}"
		} else {
			println "No templates found for process group: ${processGroupName}"
		}
	}

	def deleteTemplate(templateId) {
		client.delete { request.uri.path = '/nifi-api/templates/' + templateId }
	}

	def getTemplates() {
		return client.get { request.uri.path = '/nifi-api/flow/templates' }
	}

	def getFlowId() {
		return client.get { request.uri.path = '/nifi-api/flow/process-groups/root' }.processGroupFlow.id
	}

	def getProcessGroupId(flowId, processGroupName) {
		def processGroups = JsonPath.parse(client.get {
			request.uri.path = "/nifi-api/process-groups/${flowId}/process-groups"
		})
		return processGroups.read("\$.processGroups[?(@.component.name == '${processGroupName}')].id").get(0)
	}

	def getVariableRegistry(processGroupName) {
		def processGroupId = getProcessGroupId(getFlowId(), processGroupName)
		return client.get({ request.uri.path = "/nifi-api/process-groups/${processGroupId}/variable-registry" })
	}

	// Needed to generate the correct JSON for updating a process group's variable registry
	class ProcessGroupVariable {
		String name
		String value
	}

	/**
	 * @param processGroupName
	 * @param processGroupVariables should be a map of keys/values, where keys are variable names
	 * @return
	 */
	def updateVariables(processGroupName, processGroupVariables) {
		def registry = getVariableRegistry(processGroupName)
		def currentVersionNumber = registry.processGroupRevision.version

		def theVariables = processGroupVariables.collect { new ProcessGroupVariable(name: it.key, value: it.value) }
		def theProcessGroupId = getProcessGroupId(getFlowId(), processGroupName)

		JsonBuilder json = new JsonBuilder()
		json {
			processGroupRevision {
				version currentVersionNumber
			}
			variableRegistry {
				variables(theVariables) { ProcessGroupVariable v ->
					variable {
						name v.name
						value v.value
					}
				}
				processGroupId theProcessGroupId
			}
		}

		return client.put {
			request.uri.path = "/nifi-api/process-groups/${theProcessGroupId}/variable-registry"
			request.body = json.toString()
			request.contentType = JSON[0]
		}
	}

	def getControllerServices(processGroupId) {
		return client.get { request.uri.path = "/nifi-api/flow/process-groups/${processGroupId}/controller-services" }
	}

	def getControllerService(controllerServiceId) {
		return client.get { request.uri.path = "/nifi-api/controller-services/${controllerServiceId}" }
	}

	def getControllerServiceId(processGroupId, controllerServiceName) {
		return JsonPath.parse(getControllerServices(processGroupId)).read("\$.controllerServices[?(@.component.name == '${controllerServiceName}')].id").get(0)
	}

	def updateControllerService(controllerServiceId, propertiesObject) {
		def currentVersionNumber = getControllerService(controllerServiceId).revision.version

		JsonBuilder json = new JsonBuilder()
		json {
			revision {
				version currentVersionNumber
			}
			id controllerServiceId
			component {
				id controllerServiceId
				properties propertiesObject
			}
		}

		return client.put {
			request.uri.path = "/nifi-api/controller-services/${controllerServiceId}"
			request.body = json.toString()
			request.contentType = JSON[0]
		}
	}

	def enableControllerServices(processGroupId) {
		setStatusForControllerServices(processGroupId, "ENABLED")
	}

	def disableControllerServices(processGroupId) {
		setStatusForControllerServices(processGroupId, "DISABLED")
	}

	def setStatusForControllerServices(processGroupId, status) {
		def services = JsonPath.parse(getControllerServices(processGroupId))
		                       .read("\$.controllerServices[?(@.parentGroupId == '${processGroupId}' && @.status.runStatus != '${status}')]")
		if (!services.isEmpty()) {
			println "Found ${services.size()} controller services, will set the status of each to: ${status}"
			services.each { service ->
				println "Setting status of controller service ${service.component.name} to ${status}"
				setControllerServiceRunStatus(service.component.id, status, service.revision.version)
			}
			println "Finished setting status on controller services"
		} else {
			println "No controller services in process group ${processGroupId} were found without a status of ${status}"
		}
	}

	def setControllerServiceRunStatus(controllerServiceId, status, currentVersionNumber) {
		JsonBuilder json = new JsonBuilder()
		// NiFi expects the current version number to be passed in; if it matches, then NiFi will make the change and increment the version number
		json {
			revision {
				version currentVersionNumber
			}
			state status
		}
		return client.put {
			request.uri.path = "/nifi-api/controller-services/${controllerServiceId}/run-status"
			request.body = json.toString()
			request.contentType = JSON[0]
		}
	}

	def startProcessGroup(processGroupId) {
		setProcessGroupState(processGroupId, "RUNNING")
	}

	def stopProcessGroup(processGroupId) {
		setProcessGroupState(processGroupId, "STOPPED")
	}

	def setProcessGroupState(processGroupId, theState) {
		JsonBuilder json = new JsonBuilder()
		json {
			id processGroupId
			state theState
		}
		return client.put {
			request.uri.path = "/nifi-api/flow/process-groups/${processGroupId}"
			request.body = json.toString()
			request.contentType = JSON[0]
		}
	}

	def createSnippet(flowId, processGroupId) {
		JsonBuilder snippetBuilder = new JsonBuilder()
		snippetBuilder {
			snippet {
				parentGroupId flowId
				processGroups {
					"${processGroupId}" {
						version 1
					}
				}
			}
		}
		return client.post {
			request.uri.path = '/nifi-api/snippets'
			request.body = snippetBuilder.toString()
			request.contentType = JSON[0]
		}.snippet.id
	}

	def createTemplate(processGroupId, theSnippetId, templateName) {
		JsonBuilder template = new JsonBuilder()
		template {
			name templateName
			snippetId theSnippetId
		}
		return client.post {
			request.uri.path = "/nifi-api/process-groups/${processGroupId}/templates"
			request.body = template.toString()
			request.contentType = JSON[0]
		}.template.id
	}

	def downloadTemplate(templateId) {
		return client.get { request.uri.path = "/nifi-api/templates/${templateId}/download" }
	}

	def writeTemplateToFile(template, processGroupName) {
		templatesDir.mkdirs()
		def file = new File(templatesDir, processGroupName + ".xml")
		file.setText(template)
		println "Download file to: " + file.getPath()
	}

}