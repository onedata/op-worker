/**
 * Handles all paths not handled by Router.
 *
 * It's main purpose is to handle paths without ``lang`` segment whose have
 * multiple segments, eg. ``/spaces/1/groups``. For handling single-segment
 * invalid paths, see ``lang`` route handler.
 *
 * When a first segment of handled path is a ``lang`` id, it redirects to ``not-found``,
 * because it means, that ``lang`` route did not handled the path.
 *
 * Otherwise, it redirects to path prefixed by a default ``lang`` id - it should
 * handled by ``lang`` route.
 *
 * @module routes/wildcard
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */


import ENV from '../config/environment';
import Ember from 'ember';
import langDetect from '../utils/lang-detect';

export default Ember.Route.extend({
  model(parameters) {
    let path = parameters.path;
    // example of regexp match: ['spaces/1/users', 'spaces', '/1/users']
    let pathMatch = /.*?([\w-\.]+)(.*)/.exec(path);
    let head = pathMatch[1];
    if (head && !this.get('i18n.locales').contains(head)) {
      let userLang = langDetect() || ENV.i18n.defaultLocale;
      this.transitionTo(`/${userLang}/${path}`);
    } else {
      this.transitionTo('not-found');
    }
  }
});
