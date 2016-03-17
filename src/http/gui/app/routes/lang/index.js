import Ember from 'ember';

export default Ember.Route.extend({
  afterModel() {
    this.transitionTo('spaces', this.get('i18n.locale'));
  }
});
