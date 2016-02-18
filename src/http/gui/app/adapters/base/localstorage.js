/**
 * A localstorage adapter.
 * Using DS.LSAdapter class which should be defined in vendor JS when using
 * ember-localstorage-adapter Ember and Bower package.
 * @module adapters/base/localstorage
 */

import DS from 'ember-data';

export default DS.LSAdapter.extend({
  namespace: 'oneprovider'
});
