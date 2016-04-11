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
import bindFloater from '../utils/bind-floater';

export default Ember.Component.extend({
  activeSpace: null,
  spacesMenuService: Ember.inject.service('spaces-menu'),
  store: Ember.inject.service('store'),
  notify: Ember.inject.service('notify'),
  oneproviderServer: Ember.inject.service('oneproviderServer'),
  commonModals: Ember.inject.service('commonModals'),

  spaces: [],

  /*** Bind with main-menu service, TODO: mixin or something? ***/
  SERVICE_API: ['selectSpace', 'clearSpaceSelection', 'selectSubmenu'],

  /*** Variables for actions and modals ***/

  isCreatingSpace: false,
  newSpaceName: null,

  isJoiningSpace: false,
  joinSpaceToken: null,

  spaceToRename: null,
  renameSpaceName: null,

  spaceToRemove: null,


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
        icon: 'home',
        label: i18n.t('components.spacesMenu.drop.setHome'),
        action: 'setAsHome'
      },
      {
        icon: 'leave-space',
        label: i18n.t('components.spacesMenu.drop.leave'),
        action: 'leaveSpace'
      },
      {
        icon: 'rename',
        label: i18n.t('components.spacesMenu.drop.rename'),
        action: 'renameSpace'
      },
      {
        icon: 'remove',
        label: i18n.t('components.spacesMenu.drop.remove'),
        action: 'removeSpace'
      },
      {
        icon: 'user-add',
        label: i18n.t('components.spacesMenu.drop.inviteUser'),
        action: 'inviteUser'
      },
      {
        icon: 'group-invite',
        label: i18n.t('components.spacesMenu.drop.inviteGroup'),
        action: 'inviteGroup'
      },
      {
        icon: 'support',
        label: i18n.t('components.spacesMenu.drop.getSupport'),
        action: 'getSupport'
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

  // TODO: this method searches for newly inserted space-dropdown-menus,
  // maybe dropdown should be separate component to bind itself on insert
  bindSpaceDrops: function() {
    Ember.run.scheduleOnce('afterRender', this, function() {
      let floaters = this.$().find('.space-dropdown-menu').not('.floater');
      let sidebar = $('.secondary-sidebar');
      floaters.each(function(index, el) {
        el = $(el);
        let updater = bindFloater(el, null, {
          offsetX: 8
        });
        sidebar.on('scroll', updater);
        el.on('mouseover', updater);
        el.closest('.settings-dropdown').on('click', function() {
          window.setTimeout(() => {
            updater();
          }, 50);
        });
      });
    });
  }.observes('spaces.length'),

  spacesChanged: function() {
    if (!this.get('activeSpace')) {
      if (this.get('spaces.length') > 0) {
        let spaceToGo = this.get('spaces').find((s) => s.get('isDefault'));
        if (spaceToGo) {
          this.sendAction('showSpaceOptions', spaceToGo);
        }
      }

    }
  }.observes('spaces.length'),

  didInsertElement() {
    // reset spaces expanded state
    this.get('spaces').forEach((s) => s.set('isExpanded', false));
    this.bindSpaceDrops();
    this.spacesChanged();
  },

  spaceActionMessage(notifyType, messageId, spaceName) {
    let message = this.get('i18n').t(`components.spacesMenu.notify.${messageId}`, {spaceName: spaceName});
    this.get('notify')[notifyType](message);
  },

  actions: {
    /** Delegate to goToSpace action, should show submenu to configure Space */
    showSpaceOptions(space) {
      // TODO: a hack for firefox
      let shouldAct = true;
      if (navigator.userAgent.toLowerCase().indexOf('firefox') <= -1)
      {
        shouldAct = !event.path.find((el) => $(el).hasClass('oneicon-settings'));
      }

      if (shouldAct) {
        this.set('activeSpace', space);
        this.get('spaces').forEach((s) => s.set('isExpanded', false));
        space.set('isExpanded', true);
        this.sendAction('showSpaceOptions', space);
      }
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
      this.set('joinSpaceToken', null);
      this.set('isJoiningSpace', true);
    },

    submitJoinSpace() {
      try {
        let token = this.get('joinSpaceToken').trim();
        // TODO: loading gif in modal?
        this.get('oneproviderServer').joinSpace(token).then(
          (spaceName) => {
            this.spaceActionMessage('info', 'joinSuccess', spaceName);
          },
          (errorJson) => {
            console.log(errorJson.message);
            let message = this.get('i18n').t('components.spacesMenu.notify.joinFailed', {errorDetails: errorJson.message});
            this.get('notify')['error'](message);
            //this.spaceActionMessage('error', 'joinFailed', message);
          }
        );
      } finally {
        this.set('isJoiningSpace', false);
      }
    },

    setAsHome(space) {
      this.get('spaces').filter((s) => s.get('isDefault')).forEach((s) => {
        s.set('isDefault', false);
        s.save();
      });
      space.set('isDefault', true);
      // TODO: notify success
      space.save();
    },

    leaveSpace(space) {
      this.set('spaceToLeave', space);
    },

    submitLeaveSpace() {
      try {
        let space = this.get('spaceToLeave');
        let spaceName = space.get('name');
        this.get('oneproviderServer').leaveSpace(space).then(
          () => {
            this.spaceActionMessage('info', 'leaveSuccess', spaceName);
          },
          () => {
            this.spaceActionMessage('error', 'leaveFailed', spaceName);
          }
        );
      } finally {
        this.set('spaceToLeave', null);
      }
    },

    renameSpace(space) {
      this.set('renameSpaceName', space.get('name'));
      this.set('spaceToRename', space);
    },

    submitRenameSpace() {
      try {
        let space = this.get('spaceToRename');
        space.set('name', this.get('renameSpaceName'));
        // TODO: save notification
        space.save();
      } finally {
        this.set('spaceToRename', null);
      }
    },

    removeSpace(space) {
      this.set('spaceToRemove', space);
    },

    submitRemoveSpace() {
      try {
        let space = this.get('spaceToRemove');
        let spaceName = space.get('name');
        space.destroyRecord().then(
          () => {
            this.spaceActionMessage('info', 'removeSuccess', spaceName);
          },
          () => {
            this.spaceActionMessage('error', 'removeFailed', spaceName);
          }
        );
      } finally {
        this.set('spaceToRemove', null);
      }
    },

    inviteGroup(space) {
      this.get('commonModals').openModal('token-group', {
        space: space
      });
    },

    inviteUser(space) {
      this.get('commonModals').openModal('token-user', {
        space: space
      });
    },

    getSupport(space) {
      this.get('commonModals').openModal('token-support', {
        space: space
      });
    }
  }
});
