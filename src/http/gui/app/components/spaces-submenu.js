import Ember from 'ember';

/**
 * TODO: doc
 * External actions:
 * - showUsersConfig
 * - showGroupsConfig
 */
export default Ember.Component.extend({
  activeOption: null,
  spacesMenu: null,
  space: null,

  items: [
    {type: 'user', label: 'users', icon: 'user'},
    {type: 'group', label: 'groups', icon: 'groups'},
    {type: 'provider', label: 'providers', icon: 'provider'},
  ],

  sidebarEntryId: function() {
    return this.get('space').get('sidebarEntryId');
  }.property('space'),

  clearSelection() {
    $(`#${this.get('sidebarEntryId')} .submenu li.active`).removeClass('active');
  },

  actions: {
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
