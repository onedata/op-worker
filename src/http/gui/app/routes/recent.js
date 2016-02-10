/**
 * Currently, this is a prototype of File Brower App.
 * It allows to browse, create and view content (text) of files and directories.
 * @module routes/spaces
 * @author Łukasz Opioła
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Route.extend({
  model: function () {
    return this.store.findAll('file');
  },

  actions: {
    createNewFile: function(name, type, parentID) {
      var file = this.get('store').createRecord('file', {
        name: name,
        type: type,
        parentId: parentID
      });
      file.save();
    }
  },
});
