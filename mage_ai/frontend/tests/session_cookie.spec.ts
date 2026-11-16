import { expect, test } from './base';

/*
 * The session cookie (I-25): the server sets oauth_token on sign-in as HttpOnly with
 * SameSite=Strict, so scripts in the page cannot read it, and clears it on sign-out.
 * Every other test signs in the same way, so its API calls, websockets and terminal use it.
 */

test('the session cookie is out of the page scripts\' reach and goes on sign-out', async ({
  page,
}) => {
  const cookies = await page.context().cookies();
  const session = cookies.find(cookie => cookie.name === 'oauth_token');
  expect(session?.httpOnly).toBe(true);
  expect(session?.sameSite).toBe('Strict');
  expect(cookies.filter(cookie => cookie.name === 'oauth_token')).toHaveLength(1);

  const readable = await page.evaluate(() => document.cookie);
  expect(readable).not.toMatch(/(^|; )oauth_token=/);
  expect(readable).toMatch(/(^|; )oauth_token_expires=\d+/);

  // The API answers with the cookie alone: the page sends no token.
  const status = await page.evaluate(async () => {
    const response = await fetch('/api/pipelines?api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2');
    return (await response.json()).error?.code ?? 200;
  });
  expect(status).toBe(200);

  // Signing out, as the header's menu does: the server expires the session and clears
  // the cookies, which the page could not remove itself.
  const signedOut = await page.evaluate(async () => {
    const response = await fetch('/api/sessions/1?api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2', {
      body: JSON.stringify({ session: {} }),
      headers: { 'Content-Type': 'application/json' },
      method: 'PUT',
    });
    return (await response.json()).error?.code ?? 200;
  });
  expect(signedOut).toBe(200);
  await expect.poll(async () => (await page.context().cookies())
    .filter(cookie => cookie.name.startsWith('oauth_token')).length).toBe(0);
});
