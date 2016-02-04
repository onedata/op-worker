<!--
    This is a prototype template for file-tree-node component - a component
    that renders a single file or directory in tree view, and recursively
    renders the same component with its children (if it is a dir).
-->


import Ember from 'ember';

export default Ember.Component.extend({
  // Sorting of files by type and name
  sortProperties: ['type:asc', 'name:asc'],
  sortedChildren: Ember.computed.sort('file.children', 'sortProperties'),

  actions: {
    clickAction: function (file) {
      this.sendAction('clickAction', file);
    },

    // Components are nested, so we need to bubble the action up
    // until it reaches the controller.
    // @todo - can it be done better?
    fileClicked: function (file) {
      this.sendAction('clickAction', file);
    }
  }
});
