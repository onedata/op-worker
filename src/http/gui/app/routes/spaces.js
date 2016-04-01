/**
 * A Spaces page from main-menu.
 *
 * Could list a Spaces for user, who can select the Space and configure it.
 * @module routes/spaces
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Route.extend({
  mainMenuService: Ember.inject.service('main-menu'),

  activate() {
    console.debug('spaces activate');
    Ember.run.scheduleOnce('afterRender', this, function() {
      console.debug('select spaces');
      this.get('mainMenuService').trigger('selectItem', 'spaces');
      $('nav.secondary-sidebar').addClass('visible');
      return true;
    });
    return true;
  },

  deactivate() {
    console.debug('spaces deactivate');
    Ember.run.scheduleOnce('afterRender', this, function() {
      console.debug('deselect spaces');
      this.get('mainMenuService').trigger('deselectItem', 'spaces');
      return true;
    });
    return true;
  },

  model() {
    return this.store.findAll('space');
  },

  //afterModel(spaces) {
  //  this.transitionTo('spaces.show', spaces.find((s) => s.get('isDefault')) || spaces[0]);
  //},

  actions: {
    /** Show submenu for Space */
    goToSpace(space) {
      this.transitionTo('spaces.show', space);
    },

    /** Show users permissions table using route */
    goToUsers(space) {
      this.transitionTo('spaces.show.users', space);
    },

    /** Show groups permissions table using route */
    goToGroups(space) {
      this.transitionTo('spaces.show.groups', space);
    },
  }
});
