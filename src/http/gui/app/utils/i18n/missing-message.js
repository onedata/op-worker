/**
 * Defines what to do, when i18n message is missing. It is used by ember-i18n.
 * @see https://github.com/jamesarosen/ember-i18n/wiki/Doc:-Missing-Translations
 *
 * @module utils/i18n/missing-message
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

export default function(/*locale, key, context*/) {
  return '<missing translation>';
}
