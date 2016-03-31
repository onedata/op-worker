import DS from 'ember-data';

/**
 * A base DS.Model for creating permission models.
 * @module models/base/permissions-base
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/
export default DS.Model.extend({
  space: DS.belongsTo('space', {async: true}),

  /* Permission flags - grant permission when true.
   *
   * Note that corresponding mod* flag should be modified on these flag changes.
   */

  // TODO: documentation of permissions meaning

  permViewSpace: DS.attr('boolean', {defaultValue: false}),
  permModifySpace: DS.attr('boolean', {defaultValue: false}),
  permRemoveSpace: DS.attr('boolean', {defaultValue: false}),
  permInviteUser: DS.attr('boolean', {defaultValue: false}),
  permRemoveUser: DS.attr('boolean', {defaultValue: false}),
  permInviteGroup: DS.attr('boolean', {defaultValue: false}),
  permRemoveGroup: DS.attr('boolean', {defaultValue: false}),
  permSetPrivileges: DS.attr('boolean', {defaultValue: false}),
  permInviteProvider: DS.attr('boolean', {defaultValue: false}),
  permRemoveProvider: DS.attr('boolean', {defaultValue: false}),

  /* Modification flags (not persisted) - if true, the corresponding perm*
   * attribte was modified in view but not saved.
   *
   * Note that these flags are not mainained on perm* flags change. Maybe TODO
   */

  modViewSpace: false,
  modModifySpace: false,
  modRemoveSpace: false,
  modInviteUser: false,
  modRemoveUser: false,
  modInviteGroup: false,
  modRemoveGroup: false,
  modSetPrivileges: false,
  modInviteProvider: false,
  modRemoveProvider: false,

  /** A collection of permissions and modified flags suffixes,
   *  used mainly to iterate over these flags */
  FLAG_NAMES: [
    'ViewSpace', 'ModifySpace', 'RemoveSpace', 'InviteUser', 'RemoveUser',
     'InviteGroup', 'RemoveGroup', 'SetPrivileges', 'InviteProvider', 'RemoveProvider'
  ],

  // Checks if Permission is modified using mod* flags
  // Returns true if at least one mod* flag is true
  isModified: function() {
    return this.FLAG_NAMES.some(function(flagName) {
      return this.get('mod' + flagName);
    }, this);
  }.property('modViewSpace', 'modModifySpace', 'modRemoveSpace', 'modInviteUser',
    'modRemoveUser', 'modInviteGroup', 'modRemoveGroup', 'modSetPrivileges', 'modInviteProvider',
    'modRemoveProvider'),

  /*** Methods ***/

  /** Sets all permission fields to original state using mod* flags */
  reset: function() {
    this.FLAG_NAMES.forEach(function(flagName) {
      var modName = 'mod' + flagName;
      var permName = 'perm' + flagName;
      if (this.get(modName)) {
        this.set(modName, !this.get(modName));
        this.set(permName, !this.get(permName));
      }
    }, this);
  },

  // TODO: move to prototype
  // Sets all mod* flags to false
  setUnmodified: function() {
    this.FLAG_NAMES.forEach(function(flagName) {
      var modName = 'mod' + flagName;
      this.set(modName, false);
    }, this);
  },
});
