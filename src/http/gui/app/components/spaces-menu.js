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
  store: Ember.inject.service('store'),
  notify: Ember.inject.service('notify'),

  spaces: [],

  /*** Bind with main-menu service, TODO: mixin or something? ***/
  SERVICE_API: ['selectSpace', 'clearSpaceSelection', 'selectSubmenu'],

  isCreatingSpace: false,
  newSpaceName: null,

  isJoiningSpace: false,
  joinSpaceToken: null,

  /** Listen on mainMenuService's events */
  listen: function() {
    let mainMenuService = this.get('spacesMenuService');
    this.SERVICE_API.forEach(name => mainMenuService.on(name, this, name));
  }.on('init'),

  cleanup: function() {
    let mainMenuService = this.get('spacesMenuService');
    this.SERVICE_API.forEach(name => mainMenuService.off(name, this, name));
  }.on('willDestroyElement'),

  menuItems: function() {
    let i18n = this.get('i18n');
    return [
      {
        icon: 'leave-space',
        label: i18n.t('components.spacesMenu.drop.leave'),
      },
      {
        icon: 'rename',
        label: i18n.t('components.spacesMenu.drop.rename'),
      },
      {
        icon: 'remove',
        label: i18n.t('components.spacesMenu.drop.remove'),
      },
      {
        icon: 'group-invite',
        label: i18n.t('components.spacesMenu.drop.inviteGroup'),
      },
      {
        icon: 'user-add',
        label: i18n.t('components.spacesMenu.drop.inviteUser'),
      },
      {
        icon: 'support',
        label: i18n.t('components.spacesMenu.drop.getSupport'),
      }
    ];
  }.property(),

  clearSpaceSelection() {
    $('.secondary-sidebar .first-level').removeClass('active');
  },

  clearSpaceSubmenuSelection() {
    $('.secondary-sidebar .submenu li').removeClass('active');
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
    } else {
      console.error(`selectSubmenu without activeSpace`);
    }
  },

  currentSubmenuSelectionDidChange: function() {
    this.clearSpaceSubmenuSelection();
    let space = this.get('activeSpace');
    let optionName = space.get('currentMenuOption');
    let jqItem = `#${space.get('sidebarEntryId')} .submenu .${optionName}-permissions`;
    $(jqItem).addClass('active');
  }.observes('activeSpace', 'activeSpace.currentMenuOption'),

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

    startCreateSpace() {
      this.set('isCreatingSpace', true);
    },

    createSpaceModalOpened() {
      this.set('newSpaceName', null);
    },

    submitCreateSpace() {
      try {
        let s = this.get('store').createRecord('space', {
          name: this.get('newSpaceName')
        });
        s.save();
      } catch (error) {
        this.get('notify').error(`Creating space with name "${this.get('newSpaceName')}" failed`);
        console.error(`Space create failed: ${error}`);
      } finally {
        this.set('isCreatingSpace', false);
      }
    },

    startJoinSpace() {
      this.set('isJoiningSpace', true);
      this.set('joinSpaceToken', null);
      // TODO: get token from rpc call
      this.set('joinSpaceToken', 'fkfd0fudf89dnafydayfgsdafyudasgnf');
    },

    joinSpaceModalOpened() {
      // currently all actions in startJoinSpace
    },
  }
});
