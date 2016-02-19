/**
 * Main javascript file that initializes the prototype app.
 * @file
 * @author Łukasz Opioła
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';
import Resolver from 'ember/resolver';
import loadInitializers from 'ember/load-initializers';
import config from './config/environment';

let App;

Ember.MODEL_FACTORY_INJECTIONS = true;

App = Ember.Application.extend({
  modulePrefix: config.modulePrefix,
  podModulePrefix: config.podModulePrefix,
  Resolver
});

loadInitializers(App, config.modulePrefix);

// If the app is started in development mode, load livereload script.
if (config.environment === 'development') {
  var head = document.getElementsByTagName('head')[0];
  var script = document.createElement('script');
  script.type = 'text/javascript';
  script.src = location.protocol + '//' + (location.host || 'localhost').split(':')[0] +
    ':35729/livereload.js?snipver=1';
  head.appendChild(script);
  console.log("Loaded livereload script");
}

export default App;
