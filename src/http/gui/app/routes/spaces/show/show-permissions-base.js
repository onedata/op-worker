/**
 * An abstract class for routes that show configurable list of permissions.
 *
 * Abstract methods/properties to implement in subclasses:
 * - collectionName - String property - a name of collection from spaces (users or groups)
 *
 * @module routes/spaces/show/show-permissions-base
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

/**
 * Callback used when saving permissions table succeeded.
 *
 * @param {SpaceUserPermission} permission - a saved model
 * @private
 */
let onSaveSuccess = function(permission) {
  console.debug('permission ' + permission + ' saved successfully');
  permission.setUnmodified();
};

/**
 * Callback used when saving permissions table failed.
 *
 * @param {SpaceUserPermission} permission - a model that saving failed
 * @private
 */
let onSaveFailure = function(permission) {
  console.debug('permission ' + permission + ' saving failed!');
};

export default Ember.Route.extend({
  spacesMenuService: Ember.inject.service('spaces-menu'),
  oneproviderServer: Ember.inject.service('oneproviderServer'),

  /** Abstract: <string> should be set in subclasses, eg. user, group */
  permissionsType: null,

  model() {
    var space = this.modelFor('spaces.show');
    return {
      space: space,
      permissions: space.get(this.get('collectionName'))
    };
  },

  collectionName: function() {
    return `${this.get('permissionsType')}Permissions`;
  }.property('permissionsType'),

  activate() {
    let permissionsType = this.get('permissionsType');
    console.debug(`show ${permissionsType} activate`);
    Ember.run.scheduleOnce('afterRender', this, function() {
      console.debug(`will trigger selectSubmenu for show ${permissionsType}`);
      this.get('spacesMenuService').trigger('selectSubmenu', permissionsType);
      $('nav.secondary-sidebar').removeClass('visible');
    });
    return true;
  },

  deactivate() {
    Ember.run.scheduleOnce('afterRender', this, function() {
      $('nav.secondary-sidebar').addClass('visible');
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

    /*
     * TODO: try to save User model and check if permissions will be saved too
     * TODO: saving failure should be handled in onSaveFailure
     * there should be at least warning and table line revert
     */
    /** Save all permission models in table */
    saveChanges: function() {
      var allPermissions = this.model;
      allPermissions.forEach(function(permission) {
        if (permission.get('isModified')) {
          permission.save().then(onSaveSuccess, onSaveFailure);
        }
      });
    },
    /** Bring back all permission models from table to state before user modification */
    discardChanges: function() {
      var allPermissions = this.model;
      allPermissions.forEach(function(permission) {
        permission.reset();
      });
    },
  }
});
