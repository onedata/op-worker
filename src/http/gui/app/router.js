/**
 * Defines Ember routes - see code of Router.map callback function for details.

 * @module router
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';
import config from './config/environment';

const Router = Ember.Router.extend({
  location: config.locationType
});

Router.map(function() {
  this.route('lang', {path: '/:localeId'}, function() {
    this.route('recent', {resetNamespace: true});

    // spaces/ - all spaces configuration reached from primary sidebar
    this.route('spaces', {resetNamespace: true}, function() {
      // spaces/:space_id - entry for configuration of the single space
      this.route('show', {path: ':space_id'}, function() {
        // spaces/:space_id/users - configure users permissions for space
        this.route('users');
        // spaces/:space_id/groups - configure groups permissions for space
        this.route('groups');
      });
    });

    // data/ - list of Spaces - user can select the Space
    this.route('data', function() {
      // data/:space_id - show contents of a root dir of selected Space
      this.route('space', {path: ':space_id'}, function() {
        // data/:space_id/:file_id - select a file in a browser
        this.route('file', {path: ':file_id'});
      });
      this.route('space', function() {
        this.route('file');
      });
    });

    this.route('links', {resetNamespace: true});
    this.route('collection', {resetNamespace: true});
    this.route('trash', {resetNamespace: true});
    this.route('wildcard', { path: "*path"});
    this.route('not-found');
  });
});

export default Router;
