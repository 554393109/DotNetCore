using Microsoft.EntityFrameworkCore;

namespace CySoft
{
    public class Person
    {
        public int Id { get; set; }

        public string Name { get; set; }
    }

    public class AppDbContext : DbContext
    {
        public const string ConnectionString = "Data Source=192.168.1.62;Initial Catalog=UnifiedPay_TestOrderMQ;Persist Security Info=True;User ID=sa;Password=sa;";

        public DbSet<Person> Persons { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(ConnectionString);
        }
    }
}
