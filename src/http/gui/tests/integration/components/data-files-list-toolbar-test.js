import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('data-files-list-toolbar', 'Integration | Component | data files list toolbar', {
  integration: true
});

test('it renders', function(assert) {
  
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });" + EOL + EOL +

  this.render(hbs`{{data-files-list-toolbar}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#data-files-list-toolbar}}
      template block text
    {{/data-files-list-toolbar}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
