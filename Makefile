DESTDIR=
PREFIX=/usr
REL=drbdmanage-proxmox-$(VERSION)

export PERLDIR=${PREFIX}/share/perl5

all:
	@echo "The only useful target is 'deb'"

deb:
	dh_clean
	debuild -us -uc -i -b

install:
	install -D -m 0644 ./DRBDPlugin.pm.divert ${DESTDIR}$(PERLDIR)/PVE/Storage/DRBDPlugin.pm
	install -D -m 0644 ./DRBDPlugin.pm ${DESTDIR}$(PERLDIR)/PVE/Storage/Custom/DRBDPlugin.pm

ifndef VERSION
debrelease:
	$(error environment variable VERSION is not set)
else
debrelease:
	dh_clean
	ln -s . $(REL) || true
	tar --owner=0 --group=0 -czvf $(REL).tar.gz \
		$(REL)/Makefile \
		$(REL)/DRBDPlugin.pm \
		$(REL)/DRBDPlugin.pm.divert \
		$(REL)/debian
	if test -L "$(REL)"; then rm $(REL); fi
endif
