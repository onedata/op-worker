// Top router for the prototype app.

import Ember from 'ember';
import config from './config/environment';

const Router = Ember.Router.extend({
  location: config.locationType
});

Router.map(function() {
  // TODO: a route from VFS-1508 (not using data - it will be new data)
  this.route('recent');

  // spaces/ - all spaces configuration reached from primary sidebar
  this.route('spaces', function() {
    // spaces/:space_id - entry for configuration of the single space
    this.route('show', {path: ':space_id'}, function() {
      // spaces/:space_id/users - configure users permissions for space
      this.route('users');
      // spaces/:space_id/groups - configure groups permissions for space
      this.route('groups');
    });
  });
  this.route('data');
  this.route('links');
  this.route('collection');
  this.route('trash');
});

export default Router;
