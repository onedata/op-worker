// Prototype ember controller for the component file-tree-node.
// Here user actions are handled.

App.FileTreeNodeComponent = Ember.Component.extend({
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