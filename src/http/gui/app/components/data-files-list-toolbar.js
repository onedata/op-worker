import Ember from 'ember';

export default Ember.Component.extend({
  tagName: 'ul',
  classNames: ['nav', 'navbar-nav', 'navbar-right', 'toolbar-group'],
  items: [
    {
      icon: 'folder-new'
    },
    {
      icon: 'share'
    },
    {
      icon: 'rename'
    },
    {
      icon: 'lock'
    },
    {
      icon: 'copy'
    },
    {
      icon: 'cut'
    },
    {
      icon: 'remove'
    },
  ]
});
