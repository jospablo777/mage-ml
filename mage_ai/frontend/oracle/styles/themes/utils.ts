// @ts-ignore
import Cookies from 'js-cookie';
import ServerCookie from 'next-cookies';

import darkPalette from '@oracle/styles/themes/darkPalette';
import lightPalette from '@oracle/styles/themes/light';
import { SHARED_OPTS } from '@api/utils/token';
import {
  THEME_COOKIE,
  THEME_DARK,
  THEME_LIGHT,
  ThemeModeEnum,
  themeModeFromCookie,
} from '@oracle/styles/themes/mode';

export const LOCAL_STORAGE_KEY_THEME = THEME_COOKIE;
const V2_THEME_SETTINGS = 'theme_settings';

export function getThemeMode(ctx?: any): ThemeModeEnum {
  const value = ctx ? ServerCookie(ctx)?.[THEME_COOKIE] : Cookies.get(THEME_COOKIE);
  return themeModeFromCookie(value);
}

export function paletteForMode(mode: ThemeModeEnum) {
  return mode === ThemeModeEnum.LIGHT ? lightPalette : darkPalette;
}

export function getCurrentTheme(ctx?: any) {
  return paletteForMode(getThemeMode(ctx));
}

export function getCurrentInvertedTheme(ctx?: any) {
  return getThemeMode(ctx) === ThemeModeEnum.LIGHT ? darkPalette : lightPalette;
}

// Saves the choice and reloads, since styles computed when the app loaded use the palette.
export function setThemeMode(mode: ThemeModeEnum, reload: boolean = true) {
  // @ts-ignore
  Cookies.set(THEME_COOKIE, String(mode === ThemeModeEnum.LIGHT ? THEME_LIGHT : THEME_DARK), {
    ...SHARED_OPTS,
    expires: 9999,
  });
  try {
    // The v2 pages keep their own settings; give them the same mode.
    const settings = JSON.parse(decodeURIComponent(Cookies.get(V2_THEME_SETTINGS) || '{}'));
    // @ts-ignore
    Cookies.set(V2_THEME_SETTINGS, JSON.stringify({ ...settings, mode, theme: undefined }), {
      ...SHARED_OPTS,
      expires: 9999,
    });
  } catch {
    // An unreadable v2 cookie keeps the v2 default.
  }
  if (reload && typeof window !== 'undefined') {
    window.location.reload();
  }
}

export function setCurrentTheme(theme: number) {
  setThemeMode(Number(theme) === THEME_LIGHT ? ThemeModeEnum.LIGHT : ThemeModeEnum.DARK);
}

export function toggleTheme() {
  setThemeMode(getThemeMode() === ThemeModeEnum.LIGHT ? ThemeModeEnum.DARK : ThemeModeEnum.LIGHT);
}
