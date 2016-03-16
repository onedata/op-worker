/**
 * Handles ``/:lang_id/<single_segment>`` paths.
 * The ``:lang_id`` should be a locale id, eg. ``en``.
 * It is used to set the i18n.locale in memory (i18n service).
 *
 * If the :lang_id is not a valid language id (valid languages are fetched from
 * i18n.locales), it redirects to the same route but with :lang_id set to
 * default locale (fetched from ENV.i18n.defaultLocale).
 *
 * Handling of invalid paths with more segments, eg. /spaces/1/groups
 * is made in ``wildcard`` route.
 *
 * @module routes/lang
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';
import ENV from '../config/environment';
import langDetect from '../utils/lang-detect';

export default Ember.Route.extend({
  locales: function() {
    return this.get('i18n.locales');
  }.property('i18n.locales'),

  localeId: null,

  model(params) {
    this.set('localeId', params.localeId);
  },

  afterModel() {
    let localeId = this.get('localeId');
    if (!localeId) {
      let userLang = langDetect() || ENV.i18n.defaultLocale;
      this.set('i18n.locale', userLang);
    } else if (this.get('locales').contains(localeId)) {
      this.set('i18n.locale', localeId);
    } else {
      let userLocale = langDetect() || ENV.i18n.defaultLocale;
      // this is not a langId but rather a route without lang prefix
      let targetRoute = localeId;
      this.transitionTo(targetRoute, userLocale);
    }
  }
});
