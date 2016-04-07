/**
 * A submenu for spaces-menu, displaying options page for Space permissions:
 * users, groups or providers
 *
 * Send actions:
 * - showUsersConfig(space)
 * - showGroupsConfig(space)
 *
 * @module components/spaces-submenu
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';

export default Ember.Component.extend({
  activeOption: null,
  spacesMenu: null,
  space: null,

  items: [
    {type: 'user', labelI18n: 'components.spacesSubmenu.users', icon: 'user'},
    {type: 'group', labelI18n: 'components.spacesSubmenu.groups', icon: 'group'},
    {type: 'provider', labelI18n: 'components.spacesSubmenu.providers', icon: 'provider'},
  ],

  sidebarEntryId: function() {
    return this.get('space').get('sidebarEntryId');
  }.property('space'),

  clearSelection() {
    $(`#${this.get('sidebarEntryId')} .submenu li.active`).removeClass('active');
  },

  actions: {
    /** Selects active option page (send external action) */
    changeActiveOption(optionName) {
      this.set('activeOption', optionName);
      let space = this.get('space');
      // TODO: try-catch on sendAction?
      // TODO: redundancy...
      switch (optionName) {
        case 'user':
          this.sendAction('showUsersConfig', space);
          break;
        case 'group':
          this.sendAction('showGroupsConfig', space);
          break;
        default:
          break;
      }
    }
  }

});
