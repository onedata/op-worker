import DS from 'ember-data';

/** Associates a single onedata group with multiple space permissions */
export default DS.Model.extend({
  /** Name exposed in GUI */
  name: DS.attr('string'),
  /** Collection of permissions models - each for single Space */
  permissions: DS.hasMany('spaceGroupPermission', {async: true}),

  /*** Join methods ***/

  // TODO: not used yet, don't know if needed
  /** Get Space models for which the group has permissions defined */
  spaces: function() {
    return this.spaceGroupPermissions.map(function(gs) {
      return gs.space;
    });
  }.property('spaceGroupPermissions'),
});
