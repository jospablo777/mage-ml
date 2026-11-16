import Cookies from 'js-cookie';

import {
  COOKIE_DOMAIN,
  COOKIE_KEY_REF as COOKIE_KEY_REF_ORIG,
  COOKIE_PATH,
  SHARED_COOKIE_PROPERTIES,
} from '@utils/cookies/constants';

export const COOKIE_KEY: string = 'oauth_token';
export const COOKIE_KEY_REF: string = COOKIE_KEY_REF_ORIG;
export const DOMAIN: string = COOKIE_DOMAIN;
export const PATH: string = COOKIE_PATH;
export const SHARED_OPTS: any = SHARED_COOKIE_PROPERTIES;

// The server sets oauth_token on sign-in as an HttpOnly cookie, which scripts cannot read
// (I-25); the browser sends it with each request. This cookie holds only its expiry.
export const EXPIRES_COOKIE_KEY: string = 'oauth_token_expires';

// A token the pages can read: only one a page set before the server set the cookie.
export function getToken(): string | undefined {
  return Cookies.get(COOKIE_KEY);
}

// When the session cookie expires, in seconds since the epoch; 0 when signed out.
export function getSessionExpires(): number {
  return Number(Cookies.get(EXPIRES_COOKIE_KEY) || 0);
}

// Removes the cookies scripts can remove; the server clears the HttpOnly one on sign-out.
export function removeToken(basePath?: string) {
  Cookies.remove(
    COOKIE_KEY,
    {
      ...SHARED_OPTS,
      path: basePath || PATH,
    },
  );
  // Set by the server without a Domain, so removed without one.
  Cookies.remove(EXPIRES_COOKIE_KEY, { path: '/' });
}

// The server set the session cookie with its sign-in answer; a readable copy a page set
// before is removed so the two are not both sent.
export function setToken(
  _token: string,
  basePath?: string,
) {
  Cookies.remove(
    COOKIE_KEY,
    {
      ...SHARED_OPTS,
      path: basePath || PATH,
    },
  );
}
