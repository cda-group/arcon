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
        description: 'Runtime for real-time analytics applications in Rust',
        siteUrl: "https://example.com",
        github: 'https://github.com/cda-group/arcon',
        githubRepositoryURL: 'https://github.com/cda-group/arcon',
        navItems: [{ title: 'Learn', url: '/learn/about' }],
        sections: ['Arcon', 'Guide', 'Advanced'],
      },
    },
  ],
};
