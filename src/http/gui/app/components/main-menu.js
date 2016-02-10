/**
 * A first sidebar on the left (fixed in desktop/tablet, toggled in mobile).
 * An entry point for main routes (e.g. /data).
 * It exposes a mainMenuService, which allows e.g. to highlight a sidebar item.
 *
 * Send actions:
 * - goToItem(itemName)
 *
 * @module components/main-menu
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Component.extend({
  mainMenuService: Ember.inject.service('main-menu'),
  currentItem: null,

  /*** Bind with main-menu service, TODO: mixin or something? ***/
  SERVICE_API: ['selectItem', 'deselectItem', 'clearSelection'],

  /** Listen on mainMenuService's events */
  listen: function() {
    let mainMenuService = this.get('mainMenuService');
    this.SERVICE_API.forEach(name => mainMenuService.on(name, this, name));
  }.on('init'),

  /** Deregister event listener from main-menu service */
  cleanup: function() {
    let mainMenuService = this.get('mainMenuService');
    this.SERVICE_API.forEach(name => mainMenuService.off(name, this, name));
  }.on('willDestroyElement'),

  didInsertElement() {
    $('nav.primary-sidebar').hover(() => {
      $('nav.primary-sidebar').toggleClass('visible');
    });
  },

  /*** Main menu interface (mainly for main-menu service) ***/

  selectItem(itemName) {
    this.clearSelection();
    $('nav.primary-sidebar li a#main-'+itemName).addClass('active');
  },

  deselectItem(itemName) {
    $('nav.primary-sidebar li a#main-'+itemName).removeClass('active');
  },

  clearSelection() {
    $('nav.primary-sidebar li a.active').removeClass('active');
  },

  currentItemDidChange: function() {
    this.clearSelection();
    let currentItem = this.get('currentItem');
    if (currentItem) {
      $(`nav.primary-sidebar li a#main-${currentItem}`).addClass('active');
    }
  }.observes('currentItem'),

  actions: {
    activateItem(itemName) {
      this.set('currentItem', itemName);
      this.sendAction('goToItem', itemName);
    }
  }
});
