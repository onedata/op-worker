/**
 * A main route, setting up whole application.
 * - clear the main-menu selection
 * @module routes/spaces
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Route.extend({
  mainMenuService: Ember.inject.service('main-menu'),

  activate() {
    console.debug('app activate');
    Ember.run.scheduleOnce('afterRender', this, function() {
      console.debug('clear selection');
      this.get('mainMenuService').trigger('clearSelection');

      return true;
    });
  },

  actions: {
    goToItem(name) {
      this.transitionTo(name);
    }
  }
});
