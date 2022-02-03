const path = require('path')

module.exports = {
  pathPrefix: '/arcon',
  plugins: [
    {
      resolve: "smooth-doc",
      options: {
        name: 'Arcon',
        slug: 'arcon',
        title: 'Hej',
        author: 'Arcon Developers',
        description: 'State-first Streaming Applications in Rust',
        siteUrl: "https://example.com",
        github: 'https://github.com/cda-group/arcon',
        githubRepositoryURL: 'https://github.com/cda-group/arcon',
        baseDirectory: path.resolve(__dirname, '../'),
        navItems: [{ title: 'Learn', url: '/learn/about' }, { title: 'Community', url: '/learn/community' }, { title: 'Docs', url: 'https://docs.rs/arcon' }],
        sections: ['Arcon', 'Guide', 'Advanced'],
      },
    },
  ],
};
