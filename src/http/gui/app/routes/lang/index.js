import Ember from 'ember';

export default Ember.Route.extend({
  afterModel() {
    this.transitionTo('data', this.get('i18n.locale'));
  }
});
