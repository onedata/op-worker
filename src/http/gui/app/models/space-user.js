import DS from 'ember-data';

/**
 * Associates a single onedata user with multiple space permissions
 * @module models/space-user
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default DS.Model.extend({
  /** Name exposed in GUI */
  name: DS.attr('string'),
  /** Collection of permissions models - each for single Space */
  permissions: DS.hasMany('spaceUserPermission', {async: true}),

  /*** Join methods ***/

  // TODO: not used yet, don't know if needed
  /** Get Space models for which the user has permissions defined */
  spaces: function() {
    return this.spaceUserPermissions.map(function(us) {
      return us.space;
    });
  }.property('spaceUserPermissions'),
});
