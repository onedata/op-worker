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
  session: Ember.inject.service('session'),
  mainMenuService: Ember.inject.service('main-menu'),
  currentItem: null,

  // NOTE: links for some menu items are constructed manually for onezone
  // so be careful when changing something in onezone, eg. /#/onezone?expand_tokens=true
  /**
    Each object in returned array is a definition of main sidebar menu item:
    ```
    {
      name: <String>, if there is no link specified, redirect to lang.<name> route when clicked,
      titleI18n: <String>, a path to i18n title id in /<lang>/translation.js,
      icon: <String>, oneicon name,
      disabled: [optional] <Boolean>, if true - menu item will not be rendered at all; false by default,
      link: [optional] <Boolean>, if specified - redirect to specified link (change window location) on click
    }
    ```
  }
  */
  menuItems: function() {
    return [
      {
        name: 'data', titleI18n: 'components.mainMenu.data', icon: 'menu-data',
      },
      {
        name: 'links', titleI18n: 'components.mainMenu.links', icon: 'menu-links',
        disabled: true
      },
      {
        name: 'recent', titleI18n: 'components.mainMenu.recent',
        icon: 'menu-recent', disabled: true
      },
      {
        name: 'collection', titleI18n: 'components.mainMenu.collection',
        icon: 'menu-collection', disabled: true
      },
      {
        name: 'trash', titleI18n: 'components.mainMenu.trash', icon: 'menu-trash',
        disabled: true
      },
      {
        name: 'spaces', titleI18n: 'components.mainMenu.spaces',
        icon: 'space-empty'
      },
      {
        name: 'groups', titleI18n: 'components.mainMenu.groups',
        icon: 'menu-groups'
      },
      {
        name: 'token', titleI18n: 'components.mainMenu.token',
        icon: 'menu-token', disabled: !this.get('onezoneUrl'),
        link: `${this.get('onezoneUrl')}?expand_tokens=true`
      },
      {
        name: 'providers', titleI18n: 'components.mainMenu.providers',
        icon: 'menu-providers', disabled: !this.get('onezoneUrl'),
        link: `${this.get('onezoneUrl')}?expand_providers=true`
      },
    ];
  }.property('onezoneUrl'),

  onezoneUrl: function() {
    return this.get('session.sessionDetails.manageProvidersURL');
  }.property('session.sessionDetails'),

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
      let item = this.get('menuItems').find((item) => item.name === itemName);
      if (item.link) {
        window.location = item.link;
      } else {
        this.sendAction('goToItem', itemName);
      }
    }
  }
});
