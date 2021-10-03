const axios = require('axios');
const sleep = require('sleep-promise');

const DEFAULT_PROJECTS_URI = '/api/v4/projects';
const DEFAULT_PROJECT_ID = 4; //id for moment project
const DEFAULT_PAGE_SIZE = 100;

const LOADED_PROJECTS_TTL = 30 * 60 * 1000; //30 minutes

const DEFAULT_PIPELINE_TIMEOUT_IN_MINUTES = 120; //two hours
const DEFAULT_PIPELINE_TIMEOUT =
  DEFAULT_PIPELINE_TIMEOUT_IN_MINUTES * 60 * 1000;
const HTTP_REQUEST_TIMEOUT = 3 * 60 * 1000; //three minutes

const DAY_IN_MILLISECONDS = 24 * 3600 * 1000;
const WEEK_IN_MILLISECONDS = DAY_IN_MILLISECONDS * 7;
const PIPELINE_WAIT_REQUEST_INTERVAL = 10 * 1000;

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
    const msg = `ERROR: ${message}: ${res.status} ${res.statusText}`;
    if (logger) {
      logger.error(message);
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

    this._pageSize = DEFAULT_PAGE_SIZE;
    this._projects = [];
    this._projectsLoadedAt = this._clock.now() - WEEK_IN_MILLISECONDS;
    this._project = null;

    this._projectsRequester = this._requesterCreator.create({
      baseURL: this._projectsUrl,
      headers: this._gitlabHeaders,
      timeout: HTTP_REQUEST_TIMEOUT
    });

    this._projectId = options.projectId || DEFAULT_PROJECT_ID;
    this._projectPath = options.projectPath;
    this._pipelinesUri = '/pipelines';
    this._jobsUri = '/jobs';
  }

  _logError(message, err) {
    if (this._logger) {
      this._logger.error(message, err);
    }
  }

  _logInfo(message) {
    if (this._logger) {
      this._logger.info(message);
    }
  }

  _logWarn(message) {
    if (this._logger) {
      this._logger.warn(message);
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
      this._requester = this._requesterCreator.create({
        baseURL: this._projectUrl,
        headers: this._gitlabHeaders,
        timeout: HTTP_REQUEST_TIMEOUT
      });
      if (!this._project) {
        const res = await this._requester.get(
          '/'
        );
        throwIfErrorResult(
          res,
          `Getting project(id=${this.projectId}) request failed` +
            `(status code: ${res.statusCode})`,
          this._logger
        );
        this._project = res.data;
      }

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
        const res = await this._projectsRequester.get(
          '/',
          {
            params: {
              page,
              'per_page': this._pageSize
            }
          }
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

  async listPipelines(howMany, username) {
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
      while (result.length < howMany) {
        const res = await this._requester.get(
          this._pipelinesUri,
          {
            params: {
              ...params,
              page
            }
          }
        );
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
    const res = await this._requester.get(
      `${this._pipelinesUri}/${id}`
    );
    throwIfErrorResult(
      res,
      `Getting pipeline ${id} failed(status code ${res.status})`,
      this._logger
    );
    return res.data;
  }

  getPipelines(ids) {
    return Promise.all(ids.map(id => this.getPipeline(id)));
  }

  async getPipelineJobs(id, includeRetried = false) {
    const res = await this._requester.get(
      `${this._pipelinesUri}/${id}/jobs`,
      {
        params: {
          'include_retried': !!includeRetried
        }
      }
    );
    throwIfErrorResult(
      res,
      `Getting jobs for pipeline ${id} failed(status code ${res.status})`,
      this._logger
    );
    return res.data;
  }

  async getJobTrace(id) {
    const res = await this._requester.get(
      `${this._jobsUri}/${id}/trace`
    );
    throwIfErrorResult(
      res,
      `Getting trace for job ${id} failed`,
      this._logger
    );
    return res.data;
  }

  async createPipeline(branch, vars) {
    let res;
    try {
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
      res = await this._requester.post(
        'pipeline',
        {
          ref: branch,
          variables: pipelineVars
        }
      );
    } catch (err) {
      this._logError('failed to post', err);
      throw err;
    }
    throwIfErrorResult(res, 'Creating pipelines failed', this._logger);
    const pipeline = res.data;
    this._logInfo(
      `GitlabApi: Created pipeline ${pipeline.web_url} on branch ${branch}`
    );
    return res.data;
  }

  async createPipelines(branch, count = 1, vars) {
    const result = [];
    for (let i = 0; i < count; ++i) {
      try {
        const pipeline = await this.createPipeline(branch, vars);
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
        const res = await this._requester.post(
          `/pipelines/${pipelineId}/canel`,
          {}
        );
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

  async waitForPipeline(
    pipeline,
    startTime,
    timeout = DEFAULT_PIPELINE_TIMEOUT
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
        await this.tryCancelPipeline(pipeline);
        pipeline.status = 'timeout';
        return pipeline;
      }
      try {
        const res = await this._requester.get(`pipelines/${pipelineId}`);
        throwIfErrorResult(
          res,
          `Getting the pipelind ${pipelineId} failed`,
          this._logger
        );
        this._logInfo(
          `status for pipeline ${pipelineId} is ${res.data.status}`
        );
        pipeline.status = res.data.status;
        if (isPipelineWaiting(res.data)) {
          this._logInfo(`Waiting for pipeline ${pipelineId} to finish`);
          await sleep(PIPELINE_WAIT_REQUEST_INTERVAL);
        }
      } catch (err) {
        this._logError(`An error occured for pipeline ${pipeline.id}`, err);
        await this.tryCancelPipeline(pipeline);
        pipeline.status = 'request_error';
        return pipeline;
      }
    }
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
    for (const pipeline of pipelines) {
      const res = await this.waitForPipeline(pipeline, startTime, timeout);
      if (result.status === 'request_error' || result.status === 'timeout') {
        result.push(res);
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

    this._logInfo('Creating pipelines');
    const pipelines = await this.createPipelines(branch, batchSize, vars);
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

module.exports = GitlabApi;

