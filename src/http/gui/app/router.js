// Top router for the prototype app.

import Ember from 'ember';
import config from './config/environment';

const Router = Ember.Router.extend({
  location: config.locationType
});

Router.map(function() {
  this.route('main', { path: '/' });
});

export default Router;
