const _ = require('lodash');
const axios = require('axios');
const sleep = require('sleep-promise');

const DEFAULT_PROJECTS_URI = '/api/v4/projects';
const DEFAULT_PROJECT_ID = 4; //id for moment project
const DEFAULT_PAGE_SIZE = 100;
const DEFAULT_MAX_PARALLEL_REQUEST = 60;

const LOADED_PROJECTS_TTL = 30 * 60 * 1000; //30 minutes

const DEFAULT_PIPELINE_TIMEOUT_IN_MINUTES = 120; //two hours
const DEFAULT_PIPELINE_TIMEOUT =
  DEFAULT_PIPELINE_TIMEOUT_IN_MINUTES * 60 * 1000; //two hours
const HTTP_REQUEST_TIMEOUT = 3 * 60 * 1000; //three minutes
const DEFAULT_BRANCH_TIMEOUT_IN_SECONDS = 300; //5 minutes

const DAY_IN_MILLISECONDS = 24 * 3600 * 1000;
const WEEK_IN_MILLISECONDS = DAY_IN_MILLISECONDS * 7;
const PIPELINE_WAIT_REQUEST_INTERVAL = 10 * 1000;
const BRANCH_WAIT_REQUEST_INTERVAL = 5 * 1000;

const pipelineWaitingStatuses = new Set([
  'created',
  'waiting_for_resource',
  'preparing',
  'pending',
  'running'
]);

const isPipelineWaiting = function(pipeline) {
  return pipelineWaitingStatuses.has(pipeline.status);
};

const throwIfErrorResult = function(res, message, logger) {
  if (res.status < 200 || res.status >= 300) {
    let data = {};
    if (typeof res.data === 'object') {
      data = res.data;
    }
    const msg = `ERROR: ${message}: ${res.status} ${res.statusText}`;
    if (logger) {
      logger.error(data, msg);
    }
    throw new Error(msg);
  }
};

class GitlabApi {
  constructor(options) {
    if (!options) {
      options = {};
    }
    const gitlab = options.gitlab || {};
    if (!gitlab.url) {
      throw new Error('Gitlab url not provided');
    }
    this._requesterCreator = options.requesterCreator || axios;
    this._gitlabUrl = gitlab.url;
    this._gitlabHeaders = gitlab.headers || {};
    this._logger = options.logger;
    this._clock = options.clock || Date;
    this._initialized = false;
    this._projectsUri = options.projectsUri || DEFAULT_PROJECTS_URI;
    this._projectsUrl = options.projectsUrl ||
      `${this._gitlabUrl}${this._projectsUri}`;
    this._maxParallelRequests =
      options.maxParallelRequests || DEFAULT_MAX_PARALLEL_REQUEST;

    this._pageSize = DEFAULT_PAGE_SIZE;
    this._projects = [];
    this._projectsLoadedAt = this._clock.now() - WEEK_IN_MILLISECONDS;
    this._project = null;
    this.projectName = null;
    this._projectId = options.projectId || DEFAULT_PROJECT_ID;
    this._logInfo(`creating projectsRequester ${this._projectsUrl}`);
    this._projectsRequester = this._requesterCreator.create({
      baseURL: this._projectsUrl,
      headers: this._gitlabHeaders,
      timeout: HTTP_REQUEST_TIMEOUT,
      validateStatus: function() { return true; }
    });

    this._projectPath = options.projectPath;
    this._pipelinesUri = '/pipelines';
    this._jobsUri = '/jobs';
  }

  _logRequest(msg, path, data) {
    if (!this._logger) {
      return;
    }
    const meta = {
      path,
      projectId: this._projectId,
      projectName: this.projectName,
      data
    };
    this._logger.info(meta, `gitlab-api -- ${msg}`);
  }

  _logError(message, err) {
    if (this._logger) {
      this._logger.error(err, `gitlab-api -- ${message}`);
    }
  }

  _logInfo(message) {
    if (this._logger) {
      this._logger.info(`gitlab-api -- ${message}`);
    }
  }

  _logWarn(message) {
    if (this._logger) {
      this._logger.warn(`gitlab-api -- ${message}`);
    }
  }

  async initialize() {
    if (!this._initialized) {
      await this._init();
    }
  }

  async _init() {
    try {
      this._initialized = false;
      if (!this._projectId) {
        if (this._projectPath) {
          const project = await this._getProjectByPath(this._projectPath);
          if (!project) {
            throw new Error(`Could not find project path ${this._projectPath}`);
          }
          this._project = project;
          this._projectId = project.id;
        } else {
          this._projectId = DEFAULT_PROJECT_ID;
        }
      }
      this._projectUrl = `${this._projectsUrl}/${this._projectId}`;
      this._logInfo(`Creating requester ${this._projectUrl}`);
      this._requester = this._requesterCreator.create({
        baseURL: this._projectUrl,
        headers: this._gitlabHeaders,
        timeout: HTTP_REQUEST_TIMEOUT,
        validateStatus: function() { return true; }
      });
      if (!this._project) {
        this._logRequest('_init requester.get', '/');
        const res = await this._requester.get(
          '/'
        );
        throwIfErrorResult(
          res,
          `Getting project(id=${this._projectId}) request failed` +
            `(status code: ${res.statusCode})`,
          this._logger
        );
        this._project = res.data;
      }
      this.projectName = this._project.name;
      this._initialized = true;
    } catch (err) {
      this._logError('Project api initialization failed', err);
      throw err;
    }
  }

  async _getProjectByPath(projectPath) {
    if (this._clock.now() - this._projectsLoadedAt > LOADED_PROJECTS_TTL) {
      await this._loadProjects();
    }
    for (const project of this._projects) {
      const webUrl = project['web_url'];
      if (webUrl && webUrl.endsWith(projectPath)) {
        return project;
      }
    }
    return null;
  }

  async _loadProjects() {
    try {
      const projects = [];
      let page = 1;
      while (true) {
        const reqConfig = {
          params: {
            page,
            'per_page': this._pageSize
          }
        };
        this._logInfo(
          '_loadProjects projectsRequester.get \'/\' with config: ' +
          `${JSON.stringify(reqConfig, null, 2)}`
        );
        const res = await this._projectsRequester.get(
          '/',
          reqConfig
        );
        throwIfErrorResult(
          res,
          `Getting projects request faild(status code: ${res.status})`,
          this._logger
        );

        projects.push(...res.data);
        if (res.data.length < this._pageSize) {
          break;
        }
      }
      this._projects = projects;
      this._projectsLoadedAt = this._clock.now();
    } catch (err) {
      this._logError('Loading projects failed.', err);
      this._projectsLoadedAt = this._clock.now() - WEEK_IN_MILLISECONDS;
      throw err;
    }
  }

  getProjectInfo() {
    return this._project;
  }

  async listPipelines(howMany, username, status, after) {
    const result = [];
    if (!Number.isFinite(howMany) || howMany <= 0) {
      throw new Error(
        'The value for how many should be a number greater than 0'
      );
    }
    try {
      let page = 1;
      const params = {
        'per_page': this._pageSize
      };
      if (username) {
        params.username = username;
      }
      if (status) {
        params['status'] = status;
      }
      if (after) {
        const afterDate = new Date(after);
        params['updated_after'] = afterDate.toISOString();
      }

      while (result.length < howMany) {
        const config = {
          params: {
            ...params,
            page
          }
        };
        this._logRequest(
          'listPipelines requester.get',
          this._pipelinesUri,
          config
        );
        const res = await this._requester.get(this._pipelinesUri, config);
        ++page;
        throwIfErrorResult(
          res,
          `Listing pipelines request failed(status code: ${res.status})`,
          this._logger
        );
        if (result.length + res.data.length > howMany) {
          result.push(...res.data.slice(0, howMany - result.length));
        } else {
          result.push(...res.data);
        }
        if (res.data.length < this._pageSize) {
          break;
        }
      }
      return result;
    } catch (err) {
      this._logError('Listing pipelines failed.', err);
      throw err;
    }
  }

  async getPipeline(id) {
    const uri = `${this._pipelinesUri}/${id}`;
    this._logRequest('getPipeline requester.get', uri);
    const res = await this._requester.get(uri);
    throwIfErrorResult(
      res,
      `Getting pipeline ${id} failed(status code ${res.status})`,
      this._logger
    );
    return res.data;
  }

  async getPipelines(ids) {
    const result = [];
    for (const chunk of _.chunk(ids, this._maxParallelRequests)) {
      result.push(
        ...(await Promise.all(chunk.map(id => this.getPipeline(id))))
      );
    }
    return result;
  }

  async getPipelineJobs(id, includeRetried = false) {
    const uri = `${this._pipelinesUri}/${id}/jobs`;
    const config = {
      params: {
        'include_retried': !!includeRetried
      }
    };
    this._logRequest('getPipelineJobs requester.get', uri, config);
    const res = await this._requester.get(uri, config);
    throwIfErrorResult(
      res,
      `Getting jobs for pipeline ${id} failed(status code ${res.status})`,
      this._logger
    );
    return res.data;
  }

  async getPipelineBridges(id) {
    const uri = `${this._pipelinesUri}/${id}/bridges`;
    this._logRequest('getPipelineBridges requester.get', uri);
    const res = await this._requester.get(
      uri
    );
    throwIfErrorResult(
      res,
      `Getting bridges for pipeline ${id} failed(status code ${res.status})`,
      this._logger
    );
    return res.data;
  }

  async getChildPipelineId(pipelineId, name) {
    const bridges = await this.getPipelineBridges(pipelineId);
    for (const bridge of bridges) {
      if (bridge['downstream_pipeline']) {
        if (!name) {
          return bridge['downstream_pipeline'].id;
        }
        if (name === bridge.name) {
          return bridge['downstream_pipeline'].id;
        }
      }
    }
  }

  async getChildPipelineIds(pipelineId) {
    const bridges = await this.getPipelineBridges(pipelineId);
    const result = [];
    for (const bridge of bridges) {
      if (bridge['downstream_pipeline']) {
        result.push(bridge['downstream_pipeline'].id);
      }
    }
    return result;
  }

  async getAllChildPipelineIds(pipelineIds) {
    const result = [];
    for (const chunk of _.chunk(pipelineIds, this._maxParallelRequests)) {
      result.push(...(await Promise.all(
        chunk.map(x => this.getChildPipelineIds(x))
      )).flat());
    }
    return result;
  }

  async getJobTrace(id) {
    const uri = `${this._jobsUri}/${id}/trace`;
    const res = await this._requester.get(uri);
    throwIfErrorResult(
      res,
      `Getting trace for job ${id} failed`,
      this._logger
    );
    return res.data;
  }

  async createPipeline(branch, vars, options = {}) {
    let res;
    const times = options.times || [0, 10, 30, 60, 180, 300];
    let logInfo = null;
    let count = 0;
    for (const t of times) {
      try {
        if (count > 0) {
          this._logInfo(`Retrying in ${t} seconds...`);
        }
        ++count;
        logInfo = null;
        if (t > 0) {
          await sleep(t * 1000);
        }
        const pipelineVars = Object.entries({
          ...(vars || {}),
          'FORCE_PIPELINE_RUN': 'TRUE'
        }).map(x => {
          return {
            key: x[0],
            value: x[1].toString(),
            'variable_type': 'env_var'
          };
        });
        const uri = 'pipeline';
        const data = {
          ref: branch,
          variables: pipelineVars
        };
        logInfo = {
          uri,
          data
        };
        res = await this._requester.post(uri, data);
        throwIfErrorResult(
          res,
          `Creating pipeline on branch '${branch}' ` +
            `with ${t} seconds delay failed`
          ,
          this._logger
        );
        const pipeline = res.data;
        this._logInfo(
          `GitlabApi: Created pipeline ${pipeline.web_url} on branch ${branch}`
        );
        return res.data;
      } catch (err) {
        this._logError('failed to post', err);
      }
    }
    if (logInfo) {
      this._logRequest(
        'Failed requester.post',
        logInfo.uri,
        logInfo.data
      );
    }
    throw new Error(`Creating pipeline on branch '${branch}' failed`);
  }

  async createPipelines(branch, count, vars) {
    if (!count) {
      count = 0;
    }
    const result = [];
    for (let i = 0; i < count; ++i) {
      try {
        const pipeline = await this.createPipeline(
          branch,
          {
            ...vars,
            PIPELINE_INDEX: `${i}`
          }
        );
        result.push(pipeline);
      } catch (err) {
        const pipelineIds = result.map(x => x.id);
        await this.tryCancelPipelines(...pipelineIds);
        this._logError('An error occurred while creating the pipelines');
        throw err;
      }
    }
    return result;
  }

  async tryCancelPipelines() {
    const args = Array.from(arguments);
    for (const pipelineId of args) {
      try {
        this._logInfo(`Trying to cancel pipeline ${pipelineId}`);
        const uri = `/pipelines/${pipelineId}/cancel`;
        const data = {};
        const res = await this._requester.post(uri, data);
        throwIfErrorResult(
          res,
          `Canceling pipeline ${pipelineId} failed`,
          this._logger
        );
        this._logInfo(`Pipeline ${pipelineId} successfully canceled`);
      } catch (err) {
        this._logWarn(
          `An error occurred while trying to cancel pipeline ${pipelineId}: ` +
          `${err}`
        );
      }
    }
  }

  async waitForBranch(
    branchName,
    timeoutInSeconds = DEFAULT_BRANCH_TIMEOUT_IN_SECONDS
  ) {
    const timeout = timeoutInSeconds * 1000;
    const uri = 'repository/branches';
    let startTime = this._clock.now();
    let count = 0;
    while (true) {
      const now = this._clock.now();
      if (now - startTime > timeout) {
        this._logError(
          `Waiting for branch ${branchName} for ${timeout} ms failed.`
        );
        return false;
      }
      try {
        const reqConfig = {
          params: {
            'search': `^${branchName}$`
          }
        };
        if (count > 0) {
          this._logInfo(
            `Sleeping for ${BRANCH_WAIT_REQUEST_INTERVAL} ms ` +
              'before trying again'
          );
        }
        await sleep(BRANCH_WAIT_REQUEST_INTERVAL);
        ++count;
        const res = await this._requester.get(uri, reqConfig);
        throwIfErrorResult(
          res,
          `Searching for branch ${branchName} failed`,
          this._logger
        );
        if (res.data.length > 0) {
          this._logInfo(
            `Branch ${branchName} info: ${JSON.stringify(res.data[0], null, 2)}`
          );
          return true;
        }
        this._logInfo(
          `Could not find branch ${branchName}`
        );
      } catch (err) {
        this._logError(
          `An error occured while searching for branch ${branchName}`,
          err
        );
        return false;
      }
    }
  }

  async waitForPipeline(
    pipeline,
    startTime,
    timeout = DEFAULT_PIPELINE_TIMEOUT,
    logPrefix = ''
  ) {
    const pipelineId = pipeline.id;
    if (!startTime) {
      startTime = this._clock.now();
    }
    while (true) {
      if (!isPipelineWaiting(pipeline)) {
        return pipeline;
      }
      const now = this._clock.now();
      if (now - startTime > timeout) {
        await this.tryCancelPipelines(pipelineId);
        pipeline.status = 'timeout';
        return pipeline;
      }
      try {
        const uri = `pipelines/${pipelineId}`;
        const res = await this._requester.get(uri);
        throwIfErrorResult(
          res,
          `Getting the pipelind ${pipelineId} failed`,
          this._logger
        );
        const logMessage = `${logPrefix}status for pipeline ` +
          `${pipelineId} is ${res.data.status}`;
        this._logInfo(logMessage);
        pipeline.status = res.data.status;
        if (isPipelineWaiting(res.data)) {
          await sleep(PIPELINE_WAIT_REQUEST_INTERVAL);
        }
      } catch (err) {
        this._logError(`An error occured for pipeline ${pipeline.id}`, err);
        await this.tryCancelPipelines(pipelineId);
        pipeline.status = 'request_error';
        return pipeline;
      }
    }
  }

  _getLogPrefix(total, processed, successes) {
    return `[${successes}, ${processed}, ${total}] `;
  }

  _logPipelines(msg, pipelines) {
    if (pipelines.length === 0) {
      return;
    }
    const urls = pipelines.map(x => `    ${x.web_url}   ${x.status}`);
    this._logInfo(msg + '\n' + urls.join('\n'));
  }

  async waitForPipelines() {
    const args = Array.from(arguments);
    let timeout = args[0];
    let sliceIndex = 1;
    if (!Number.isFinite(timeout)) {
      sliceIndex = 0;
      timeout = DEFAULT_PIPELINE_TIMEOUT;
    }
    const pipelines = args.slice(sliceIndex);
    const startTime = Date.now();
    const result = [];
    const total = pipelines.length;
    let processed = 0;
    let successes = 0;
    const failedPipelines = [];
    for (const pipeline of pipelines) {
      const lastFailedPipelines = failedPipelines.slice(-5);
      this._logPipelines(
        'Last(up to 5) failed pipeline(s):',
        lastFailedPipelines
      );
      this._logInfo(`*** waiting for pipeline ${pipeline.id} to finish ***`);
      const res = await this.waitForPipeline(
        pipeline,
        startTime,
        timeout,
        this._getLogPrefix(total, processed, successes)
      );
      ++processed;
      if (res.status === 'request_error' || res.status === 'timeout') {
        result.push(res);
      }
      if (pipeline.status === 'success') {
        ++successes;
      } else {
        failedPipelines.push(res);
      }
    }
    if (result.lengths > 0) {
      this._logWarn(`There are ${result.length} pipeline(s) ` +
        'with abnormal status after waiting');
      result.forEach((x, i) => this._logWarn(
        `${i.toString().padStart(3)} ${x.id.toString().padStart(4)} ` +
        `${x.status()}`
      ));
    }
    return result;
  }

  async createBatch(branch, batchSize, pipelineTimeoutInMinutes, vars) {
    if (pipelineTimeoutInMinutes === undefined) {
      pipelineTimeoutInMinutes = process.env['PIPELINE_TIMEOUT'] ||
        DEFAULT_PIPELINE_TIMEOUT_IN_MINUTES;
    }
    const pipelineTimeout = pipelineTimeoutInMinutes * 60 * 1000;

    this._logInfo(`Creating pipelines.Vars: ${JSON.stringify(vars, null, 2)}`);
    const pipelines = await this.createPipelines(branch, batchSize, vars);
    const logRecords = [];
    for (const pipeline of pipelines) {
      logRecords.push(`    ${pipeline.web_url}`);
    }
    this._logInfo(
      `Created ${pipelines.length} pipelines on branch ${branch}: \n` +
      logRecords.join('\n')
    );
    this._logInfo(
      `Waiting up to ${pipelineTimeoutInMinutes} minute(s) ` +
      'for pipelines to finish'
    );
    await this.waitForPipelines(
      pipelineTimeout,
      ...pipelines
    );
    const logLines = [
      `A batch of ${pipelines.length} pipeline(s) has been created:`,
      ''
    ];
    pipelines.forEach(x => {
      logLines.push(`  pipeline: ${x.id}, status: '${x.status}'`);
    });
    this._logInfo(logLines.join('\n'));
    return pipelines;
  }
}

module.exports = {
  GitlabApi,
  isPipelineWaiting
};

