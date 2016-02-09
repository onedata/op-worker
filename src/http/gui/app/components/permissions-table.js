import Ember from 'ember';


/**
 * Callback used when saving permissions table succeeded.
 *
 * @param {SpaceUserPermission} permission - a saved model
 */
var onSaveSuccess = function(permission) {
  console.debug('permission ' + permission + ' saved successfully');
  permission.setUnmodified();
};

/**
 * Callback used when saving permissions table failed.
 *
 * @param {SpaceUserPermission} permission - a model that saving failed
 */
var onSaveFailure = function(permission) {
  console.debug('permission ' + permission + ' saving failed!');
};

export default Ember.Component.extend({
  title: null,

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

    /*
     * TODO: try to save User model and check if permissions will be saved too
     * TODO: saving failure should be handled in onSaveFailure
     * there should be at least warning and table line revert
     */
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
    }
  }
});
