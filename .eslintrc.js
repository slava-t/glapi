module.exports = {
  'env': {
    'browser': true,
    'commonjs': true,
    'es6': true,
    'node': true,
    'jasmine': true,
    'jest': true,
  },
  'extends': ['eslint:recommended', 'plugin:jasmine/recommended'],
  'globals': {
    // Hack, should find a way to ignore globally defined helpers
    'wrapInRouter': 'readonly',
    'wrapInProvider': 'readonly',
  },
  'parser': 'babel-eslint',
  'parserOptions': {
    'ecmaFeatures': {
      'experimentalObjectRestSpread': true,
      'jsx': true
    },
    'sourceType': 'module',
    'ecmaVersion': 2018,
  },
  'plugins': ['react', 'jasmine', 'jest'],
  'rules': {
    'array-bracket-spacing': ['error', 'never'],
    'block-spacing': ['error', 'always'],
    'brace-style': ['error', '1tbs', {
      'allowSingleLine': true,
    }],
    'camelcase': ['error'],
    'comma-spacing': ['error'],
    'comma-style': ['error', 'last'],
    'consistent-this': ['error', 'self'],
    'curly': ['error', 'all'],
    'dot-location': ['error', 'property'],
    'func-call-spacing': ['error', 'never'],
    'func-names': ['error', 'never'],
    'func-style': ['error', 'expression'],
    'function-paren-newline': ['error', 'consistent'],
    'indent': ['error', 2, {
      'SwitchCase': 1,
      'MemberExpression': 2,
      // we are using jsx-indent to manage indents in jsx, so we ignore it here
      // TODO: HACK if statements don't work correctly at the moment see
      // so we ignore anything inside the test
      // https://github.com/eslint/eslint/issues/9779
      'ignoredNodes': ['JSXElement *', '.test *'],
    }],
    'jasmine/new-line-before-expect': ['off'],
    'jasmine/new-line-between-declarations': ['off'],
    'jasmine/no-disabled-tests': ['off'],
    'jasmine/no-spec-dupes': ['error', 'branch'],
    'jasmine/no-suite-dupes': ['error', 'branch'],
    'jsx-quotes': ['error', 'prefer-double'],
    'key-spacing': ['error', {
      'mode': 'strict',
    }],
    'keyword-spacing': ['error', {
      'before': true,
      'after': true,
    }],
    'linebreak-style': ['error', 'unix'],
    'max-len': ['error', {
      'code': 80,
      'ignoreUrls': true,
      'ignorePattern': '^import ',
    }],
    'no-alert': ['error'],
    'no-catch-shadow': ['error'],
    'no-console': ['error', {
      'allow': ['warn', 'error'],
    }],
    'no-constant-condition': ['error', {
      'checkLoops': false,
    }],
    'no-lonely-if': ['error'],
    'no-redeclare': ['off'],
    'no-return-assign': ['error', 'always'],
    'no-return-await': ['error'],
    'no-sequences': ['error'],
    'no-shadow': ['error'],
    'no-trailing-spaces': ['error'],
    'no-unmodified-loop-condition': ['error'],
    'no-unused-expressions': ['error', {
      'allowShortCircuit': true,
    }],
    'no-unused-vars': ['error', {
      'vars': 'all',
      'args': 'after-used',
      'ignoreRestSiblings': true,
    }],
    'no-whitespace-before-property': ['error'],
    'object-curly-newline': ['error', {
      'consistent': true,
    }],
    'object-curly-spacing': ['error', 'never'],
    'one-var': ['error', 'never'],
    'operator-linebreak': ['error', 'after'],
    'quotes': ['error', 'single'],
    'react/jsx-indent-props': ['error', 4],
    'react/jsx-uses-vars': [2],
    'react/jsx-uses-react': [2],
    'require-await': ['error'],
    'semi': ['error', 'always'],
    'semi-spacing': ['error'],
    'semi-style': ['error', 'last'],
    'space-before-blocks': ['error', 'always'],
    'space-before-function-paren': ['error', {
      'anonymous': 'never',
      'named': 'never',
      'asyncArrow': 'always',
    }],
    'space-in-parens': ['error', 'never'],
    'space-infix-ops': ['error'],
    'switch-colon-spacing': ['error'],
    'yoda': ['error'],
  }
};
