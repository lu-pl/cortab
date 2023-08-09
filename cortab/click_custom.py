"""Custom click subclasses."""

import click


class RequiredIf(click.Option):
    """...

    Inspired by https://stackoverflow.com/a/51235564/6455731.
    """

    def __init__(self, *args, required_if: str = None, ** kwargs):
        """..."""
        self._required_if = required_if
        super().__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        """..."""
        this_option_present = self.name in opts
        required_option_present = self._required_if in opts

        if required_option_present and not this_option_present:
            raise click.UsageError(
                f"Missing required option '--{self.name}'.")

        return super().handle_parse_result(ctx, opts, args)


class MultiOptions(click.Option):
    """Allows for multiple click option.

    From https://stackoverflow.com/a/48394004/6455731.
    Todo: this is messy and needs cleanup.
    """

    def __init__(self, *args, **kwargs):
        """..."""
        self.save_other_options = kwargs.pop('save_other_options', True)
        nargs = kwargs.pop('nargs', -1)
        assert nargs == -1, 'nargs, if set, must be -1 not {}'.format(nargs)
        super(MultiOptions, self).__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._eat_all_parser = None

    def add_to_parser(self, parser, ctx):
        """..."""
        def parser_process(value, state):
            """..."""
            # method to hook to the parser.process
            done = False
            value = [value]
            if self.save_other_options:
                # grab everything up to the next option
                while state.rargs and not done:
                    for prefix in self._eat_all_parser.prefixes:
                        if state.rargs[0].startswith(prefix):
                            done = True
                    if not done:
                        value.append(state.rargs.pop(0))
            else:
                # grab everything remaining
                value += state.rargs
                state.rargs[:] = []
            value = tuple(value)

            # call the actual process
            self._previous_parser_process(value, state)

        retval = super(MultiOptions, self).add_to_parser(parser, ctx)
        for name in self.opts:
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break
        return retval


class RequiredMultiOptions(RequiredIf, MultiOptions):
    """..."""

    pass


class DefaultCommandGroup(click.Group):
    """allow a default command for a group.

    From https://stackoverflow.com/a/52069546/6455731.
    """

    def command(self, *args, **kwargs):
        """..."""
        default_command = kwargs.pop('default_command', False)
        if default_command and not args:
            kwargs['name'] = kwargs.get('name', '<>')
        decorator = super(
            DefaultCommandGroup, self).command(*args, **kwargs)

        if default_command:
            def new_decorator(f):
                cmd = decorator(f)
                self.default_command = cmd.name
                return cmd

            return new_decorator

        return decorator

    def resolve_command(self, ctx, args):
        """..."""
        try:
            # test if the command parses
            return super(
                DefaultCommandGroup, self).resolve_command(ctx, args)
        except click.UsageError:
            # command did not parse, assume it is the default command
            args.insert(0, self.default_command)
            return super(
                DefaultCommandGroup, self).resolve_command(ctx, args)
