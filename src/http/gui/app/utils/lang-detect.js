/**
 * Uses various JS methods to get user preferred language.
 *
 * Based on: http://stackoverflow.com/a/26889118
 *
 * @return {string} short locale id, eg. 'en'
 *
 * @module utils/lang-detect
 * @function langDetect
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default function langDetect() {
  // let lang = window.navigator.languages ? window.navigator.languages[0] : null;
  // lang = lang || window.navigator.language || window.navigator.browserLanguage || window.navigator.userLanguage;
  //
  // if (lang.indexOf('-') !== -1) {
  //   lang = lang.split('-')[0];
  // }
  //
  // if (lang.indexOf('_') !== -1) {
  //   lang = lang.split('_')[0];
  // }
  //
  // return lang;

  // TODO: disabled for demo 3.0 to force English
  return 'en';
}
