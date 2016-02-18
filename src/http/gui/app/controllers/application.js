/**
 * An applicaiton controller, that probably will be removed.
 * @module controllers/application
 * @author Jakub Liput
 * @author Łukasz Opioła
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';

export default Ember.Controller.extend({
  serverService: Ember.inject.service('server'),

  /** @todo this should be moved to main-menu component */
  mains: [
    {name: 'data', icon: 'data'},
    {name: 'links', icon: 'links'},
    {name: 'recent', icon: 'recent'},
    {name: 'collection', icon: 'collection'},
    {name: 'trash', icon: 'trash'},
    {name: 'spaces', icon: 'spaces'},
    // TODO: bad names of icons
    {name: 'groups', icon: 'group2'},
    {name: 'token', icon: 'group'},
  ],

  activate() {
    this.initializeValue('userName');
  },

  /**
   * Initialize a value by sending a callback to the server. Value can be
   * e.g. user name which has to be checked once and then is cached.
   * @todo TODO VFS-1508 don't know if it will be necessary here
   *   (as we go away from controllers)
   */
  initializeValue: function (key) {
    // TODO VFS-1508: probably use of (returnedValue) => {...}
    var controller = this;
    this.get('serverService').callServer(key, function (returnedValue) {
      controller.set(key, returnedValue);
    });
  },

});
