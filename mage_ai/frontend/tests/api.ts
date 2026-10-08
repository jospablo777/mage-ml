import { APIRequestContext, expect } from '@playwright/test';

// Client ID of the OAuth application the server creates for the frontend. POST requests read
// it from the X-API-KEY header or the JSON body, never from the query string.
const API_KEY = 'zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2';

type Method = 'DELETE' | 'GET' | 'POST' | 'PUT';

export const OUTPUT_LOADER = `
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(*args, **kwargs):
    return pd.DataFrame({
        'id': [1, 2, 3],
        'ni': pd.array([1, None, 3], dtype='Int64'),
        'cat': pd.Categorical(['x', 'y', 'x']),
        'ts': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']).tz_localize('UTC'),
        'attrs': [{'a': 1}, {'a': 2, 'b': [1, 2]}, {'a': 3}],
        'tags': [['x'], ['y', 'z'], []],
    })
`;

export const OUTPUT_TRANSFORMER = `
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(df, *args, **kwargs):
    df = df.copy()
    df['input_dtypes'] = ','.join(f'{c}={t}' for c, t in df.dtypes.astype(str).items())
    df['n_tags'] = [len(value) for value in df['tags']]
    return df
`;

export class MageApi {
  constructor(private request: APIRequestContext, private token: string) {}

  static async signIn(
    request: APIRequestContext,
    email = 'admin@admin.com',
    password = 'admin',
  ): Promise<MageApi> {
    const response = await request.post('/api/sessions', {
      data: { session: { email, password } },
      headers: { 'X-API-KEY': API_KEY },
    });
    const body = await response.json();
    expect(body.error).toBeUndefined();

    return new MageApi(request, body.session.token);
  }

  async call(method: Method, path: string, data?: object) {
    const response = await this.request.fetch(`/api/${path}`, {
      data,
      headers: {
        Cookie: `oauth_token=${this.token}`,
        'X-API-KEY': API_KEY,
      },
      method,
    });
    const body = await response.json();
    if (body.error) {
      throw new Error(`${method} ${path}: ${JSON.stringify(body.error)}`);
    }

    return body;
  }

  async createPipeline(name: string): Promise<string> {
    const body = await this.call('POST', 'pipelines', { pipeline: { name, type: 'python' } });

    return body.pipeline.uuid;
  }

  async addBlock(
    pipelineUUID: string,
    block: { content: string; name: string; type: string; upstream_blocks?: string[] },
  ) {
    await this.call('POST', `pipelines/${pipelineUUID}/blocks`, {
      block: { language: 'python', ...block },
    });
  }

  async deletePipeline(pipelineUUID: string) {
    await this.call('DELETE', `pipelines/${pipelineUUID}`);
  }

  // Creates an active @once trigger that starts in the past and waits for its pipeline run.
  async runOnce(pipelineUUID: string, triggerName: string): Promise<{ id: number; status: string }> {
    const startTime = new Date(Date.now() - 60000)
      .toISOString()
      .replace('T', ' ')
      .slice(0, 19);
    const { pipeline_schedule: schedule } = await this.call(
      'POST',
      `pipelines/${pipelineUUID}/pipeline_schedules`,
      {
        pipeline_schedule: {
          name: triggerName,
          schedule_interval: '@once',
          schedule_type: 'time',
          start_time: startTime,
          status: 'active',
        },
      },
    );

    let run;
    await expect.poll(async () => {
      // /pipeline_runs ignores pipeline_schedule_id. The nested route filters by trigger.
      const body = await this.call('GET', `pipeline_schedules/${schedule.id}/pipeline_runs`);
      run = body.pipeline_runs[0];

      return run?.status;
    }, { intervals: [2000], timeout: 120000 }).toMatch(/^(completed|failed|cancelled)$/);

    return run;
  }
}
