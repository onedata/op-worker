/**
 * A Component to show collections of changeable checkboxes with permissions
 * for each "permission" entity. The entity can be e.g. a userPermission record.
 * Effectively, on desktop this should be a table, where each row respresents
 * single permission collection for entity.
 *
 * @module components/permissions-table
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';

/**
 * Callback used when saving permissions table succeeded.
 * @private
 * @todo the spaces/show/permissions-base route copies this method
 *
 * @param {SpaceUserPermission} permission - a saved model
 */
let onSaveSuccess = function(permission) {
  console.debug('permission ' + permission + ' saved successfully');
  permission.setUnmodified();
};

/**
 * Callback used when saving permissions table failed.
 * @private
 * @todo implement real save failure behavior
 * @todo the spaces/show/permissions-base route copies this method
 *
 * @param {SpaceUserPermission} permission - a model that saving failed
 */
let onSaveFailure = function(permission) {
  console.debug('permission ' + permission + ' saving failed!');
};

export default Ember.Component.extend({
  oneproviderServer: Ember.inject.service(),
  commonModals: Ember.inject.service(),

  classNames: ['permissions-table'],

  /**
   * Collection of permissions-base model subclasses instances.
   * Each represents a sigle entity with some permissions to set.
   *
   * It must be injected into component.
   */
  permissions: [],

  /**
   * A type of table: users/groups
   * It must be injected into component.
   */
  type: null,

  typeSingular: function() {
    return this.get('type') === 'groups' ? 'group' : 'user';
  }.property('type'),

  /** A localized title of table (based on type) */
  title: function() {
    return this.get('type') ?
      this.get('i18n').t(`spaces.show.${this.get('type')}.tableTitle`) : '';
  }.property('type'),

  inviteButton: function() {
    switch (this.get('type')) {
      case 'users':
        return 'user-add';
      case 'groups':
        return 'group-invite';
      default:
        return null;
    }
  }.property('type'),

  /** Should permissions table be treated as modified and not saved?
   *  It is true when at least one permission model in collection is modified.
   */
  isModified: function() {
    var val = this.get('permissions').any(p => p.get('isModified'));
    return val;
  }.property('permissions.@each.isModified'),

  didInsertElement() {
    this.get('permissions').forEach((p) => {
      $(`#perm-row-${p.get('owner').get('id')} .one-first`).click(() => {
        $(`#perm-row-${p.get('owner').get('id')}`).toggleClass('active');
      });
    });
  },

  actions: {
    /** Change state of single permission checkbox */
    togglePermission: function(permission, propertyName) {
      var permName = 'perm' + propertyName;
      var modName = 'mod' + propertyName;
      permission.set(permName, !permission.get(permName));
      permission.set(modName, !permission.get(modName));
    },

    /** Save all permission models in table */
    saveChanges: function() {
      this.get('permissions').forEach(function(permission) {
        if (permission.get('isModified')) {
          permission.save().then(onSaveSuccess, onSaveFailure);
        }
      });
    },

    /** Bring back all permission models from table to state before user modification */
    discardChanges: function() {
      this.get('permissions').forEach(function(permission) {
        permission.reset();
      });
    },

    invite(space) {
      // TODO: plural -> singular mess
      this.get('commonModals').openModal(`token-${this.get('typeSingular')}` , {
        space: space
      });
    },
  }
});
