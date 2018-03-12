DESTDIR=
PREFIX=/usr
REL=linstor-proxmox-$(VERSION)

export PERLDIR=${PREFIX}/share/perl5

all:
	@echo "The only useful target is 'deb'"

deb:
	dh_clean
	debuild -us -uc -i -b

install:
	install -D -m 0644 ./DRBDPlugin.pm.divert ${DESTDIR}$(PERLDIR)/PVE/Storage/DRBDPlugin.pm
	install -D -m 0644 ./LINSTORPlugin.pm ${DESTDIR}$(PERLDIR)/PVE/Storage/Custom/LINSTORPlugin.pm

ifndef VERSION
debrelease:
	$(error environment variable VERSION is not set)
else
debrelease:
	dh_clean
	ln -s . $(REL) || true
	tar --owner=0 --group=0 -czvf $(REL).tar.gz \
		$(REL)/Makefile \
		$(REL)/README.md \
		$(REL)/DRBDPlugin.pm.divert \
		$(REL)/LINSTORPlugin.pm \
		$(REL)/debian
	if test -L "$(REL)"; then rm $(REL); fi
endif
