/**
 * Here user actions are handled.
 * @todo TODO VFS-1508: better doc
 *
 * Send actions:
 * - clickAction(file)
 *
 * @module components/file-tree-node
 * @author Łukasz Opioła
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';

export default Ember.Component.extend({
  /** Sorting of files by type and name */
  sortProperties: ['type:asc', 'name:asc'],
  sortedChildren: Ember.computed.sort('file.children', 'sortProperties'),

  actions: {
    clickAction: function (file) {
      this.sendAction('clickAction', file);
    },

    /**
     * Components are nested, so we need to bubble the action up
     * until it reaches the controller.
     * @todo - can it be done better?
     */
    fileClicked: function (file) {
      this.sendAction('clickAction', file);
    }
  }
});
