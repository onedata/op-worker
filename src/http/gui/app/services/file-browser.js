import Ember from 'ember';

/**
 * A global state and set of actions for file browser elements rendered in various
 * routes: data-files-list, data-files-list-toolbar.
 * It is created to avoid implementation of similar methods in separate components
 * or making heavy implementation of actions in model (components shares only File model).
 *
 * @module services/file-browser
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

export default Ember.Service.extend({
  store: Ember.inject.service('store'),
  errorNotifier: Ember.inject.service('error-notifier'),
  fileSystemTree: Ember.inject.service('file-system-tree'),

  /**
   * Current dir opened in file browser.
   */
  dir: null,

  // TODO: multiple select only with ctrl
  selectFile(file) {
    file.set('isSelected', !file.get('isSelected'));
  },

  // TODO: should open dialog or delegate to data files list (needs bind with data-files-list)
  // renameSelectedFile() {
  // },
});
