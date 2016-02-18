/**
 * A secondary sidebar for selecting Space to modify its permissions.
 * Uses internally spaces-submenu component to render select for
 * users/groups/providers permissions.
 *
 * Send actions:
 * - showSpaceOptions(space)
 * - showUsersConfig(space)
 * - showGroupsConfig(space)
 *
 * @module components/spaces-menu
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';

export default Ember.Component.extend({
  activeSpace: null,
  spacesMenuService: Ember.inject.service('spaces-menu'),
  spaces: [],

  /*** Bind with main-menu service, TODO: mixin or something? ***/
  SERVICE_API: ['selectSpace', 'clearSpaceSelection', 'selectSubmenu'],

  /** Listen on mainMenuService's events */
  listen: function() {
    let mainMenuService = this.get('spacesMenuService');
    this.SERVICE_API.forEach(name => mainMenuService.on(name, this, name));
  }.on('init'),

  cleanup: function() {
    let mainMenuService = this.get('spacesMenuService');
    this.SERVICE_API.forEach(name => mainMenuService.off(name, this, name));
  }.on('willDestroyElement'),

  clearSpaceSelection() {
    $('.secondary-sidebar li').removeClass('active');
  },

  clearSpaceSubmenuSelection() {
    $('.secondary-sidebar li .submenu li').removeClass('active');
  },

  selectSpace(space) {
    console.debug(`selecting space: ${space}`);
    this.set('activeSpace', space);
  },

  /** Changes selection of option in currently expanded submenu of current Space
   *  Suported optionName: user, group, provider
   */
  selectSubmenu(optionName) {
    let space = this.get('activeSpace');
    console.debug(`select submenu for ${optionName} with space ${space}`);
    if (space) {
      space.set('currentMenuOption', optionName);
    }
  },

  currentSubmenuSelectionDidChange: function() {
    this.clearSpaceSubmenuSelection();
    let space = this.get('activeSpace');
    let optionName = space.get('currentMenuOption');
    let jqItem = `#${space.get('sidebarEntryId')} .submenu .${optionName}-permissions`;
    $(jqItem).addClass('active');
  }.observes('activeSpace.currentMenuOption'),

  // TODO: sender, key, value, rev... arguments
  activeSpaceDidChange: function() {
    /* jshint unused:false */
    this.clearSpaceSelection();
    let space = this.get('activeSpace');
    console.debug(`current space changed, selecting: ${space.get('sidebarEntryId')}`);
    if (space) {
      $('.secondary-sidebar')
        .find(`#${space.get('sidebarEntryId')}`)
        .addClass('active');
    }
  }.observes('activeSpace'),

  actions: {
    /** Delegate to goToSpace action, should show submenu to configure Space */
    showSpaceOptions(space) {
      this.set('activeSpace', space);
      this.sendAction('showSpaceOptions', space);
    },

    showUsersConfig(space) {
      this.sendAction('showUsersConfig', space);
    },

    showGroupsConfig(space) {
      this.sendAction('showGroupsConfig', space);
    },
  }
});
